"""
ingestion_dag.py — Data Ingestion DAG (Airflow 3.x)

Runs every 1 minute. Pipeline:
1. read_data        — pick a random CSV from raw_data/, delete it, push via XCom
2. validate_data    — Great Expectations v1.x Checkpoint + row-level checks
3a. save_statistics — PostgresHook → ingestion_stats table
3b. send_alerts     — Teams webhook (medium/high criticality only)
3c. split_and_save_data — route rows to good_data/ or bad_data/

Tasks 3a, 3b, 3c run in parallel after 2.
"""

from __future__ import annotations

import json
import os
import glob
import random
from datetime import datetime, timezone

import pandas as pd
import requests as http_requests

# Airflow 3: imports consolidated under airflow.sdk
from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RAW_DATA_DIR = "/opt/airflow/data/raw_data"
GOOD_DATA_DIR = "/opt/airflow/data/good_data"
BAD_DATA_DIR = "/opt/airflow/data/bad_data"
GX_ROOT_DIR = "/opt/airflow/gx"

TEAMS_WEBHOOK_ENV = "TEAMS_WEBHOOK_URL"

VALID_CLOUD_PROVIDERS = {"AWS", "GCP", "Azure", "IBM", "Oracle", "Alibaba"}
REQUIRED_COLUMNS = [
    "cloud_provider", "service", "severity",
    "system_load_before_outage", "number_of_customers_affected",
    "ticket_count", "backup_system_triggered",
]


def compute_criticality(total_rows: int, invalid_rows: int, has_schema_error: bool) -> str:
    """Return 'high', 'medium', 'low', or 'none'."""
    if has_schema_error:
        return "high"
    if total_rows == 0:
        return "low"
    pct = invalid_rows / total_rows
    if pct > 0.50:
        return "high"
    if pct >= 0.10:
        return "medium"
    if pct > 0:
        return "low"
    return "none"


# ---------------------------------------------------------------------------
# DAG definition (Airflow 3 decorator syntax)
# ---------------------------------------------------------------------------

@dag(
    dag_id="ingestion_dag",
    schedule="* * * * *",           # every minute
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ingestion", "data-quality"],
)
def ingestion_dag():

    # -----------------------------------------------------------------------
    # Task 1 — read_data
    # -----------------------------------------------------------------------
    @task()
    def read_data() -> dict:
        """Pick a random CSV from raw_data/, read it, delete it. Return via XCom."""
        files = glob.glob(os.path.join(RAW_DATA_DIR, "*.csv"))
        if not files:
            raise AirflowSkipException("No files in raw_data/ — skipping run.")

        chosen = random.choice(files)
        df = pd.read_csv(chosen)
        os.remove(chosen)
        print(f"[read_data] Read and deleted: {chosen} ({len(df)} rows)")

        return {
            "file_path": chosen,
            "filename": os.path.basename(chosen),
            "data_json": df.to_json(orient="records"),
            "columns": list(df.columns),
        }

    # -----------------------------------------------------------------------
    # Task 2 — validate_data
    # -----------------------------------------------------------------------
    @task()
    def validate_data(file_info: dict) -> dict:
        """Run row-level validation + GX v1.x Checkpoint. Return structured stats."""
        import great_expectations as gx

        data_json = file_info["data_json"]
        columns = file_info["columns"]
        filename = file_info["filename"]

        df = pd.read_json(data_json, orient="records")

        # ── Schema check (file-level) ─────────────────────────────────────────
        missing_cols = [c for c in REQUIRED_COLUMNS if c not in columns]
        has_schema_error = bool(missing_cols)

        # ── Row-level validation ──────────────────────────────────────────────
        row_errors: dict[int, list[str]] = {}

        for idx, row in df.iterrows():
            errs: list[str] = []

            # 1. Completeness — null in required column
            for col in REQUIRED_COLUMNS:
                if col in df.columns and pd.isna(row.get(col)):
                    errs.append("null_value")
                    break

            # 2. Validity — ticket_count out of range
            if "ticket_count" in df.columns:
                tc = row.get("ticket_count")
                try:
                    if float(tc) < 0:
                        errs.append("out_of_range")
                except (TypeError, ValueError):
                    pass

            # 3. Consistency — invalid cloud_provider
            if "cloud_provider" in df.columns:
                cp = row.get("cloud_provider")
                if isinstance(cp, str) and cp not in VALID_CLOUD_PROVIDERS:
                    errs.append("invalid_categorical")

            # 4. Type — system_load_before_outage must be numeric
            if "system_load_before_outage" in df.columns:
                val = row.get("system_load_before_outage")
                if val is not None:
                    try:
                        float(val)
                    except (TypeError, ValueError):
                        errs.append("wrong_type")

            # 5. Statistical outlier — duration_minutes
            if "duration_minutes" in df.columns:
                dm = row.get("duration_minutes")
                try:
                    if float(dm) > 100_000:
                        errs.append("statistical_outlier")
                except (TypeError, ValueError):
                    pass

            if errs:
                row_errors[int(idx)] = errs

        total_rows = len(df)
        invalid_rows = len(row_errors)
        valid_rows = total_rows - invalid_rows

        error_type_counts: dict[str, int] = {}
        for errs in row_errors.values():
            for e in errs:
                error_type_counts[e] = error_type_counts.get(e, 0) + 1
        if has_schema_error:
            error_type_counts["missing_column"] = len(missing_cols)

        criticality = compute_criticality(total_rows, invalid_rows, has_schema_error)

        # ── GX v1.x Checkpoint ───────────────────────────────────────────────
        report_url = ""
        try:
            context = gx.get_context(mode="file", project_root_dir=GX_ROOT_DIR)

            datasource = context.data_sources.add_or_update_pandas(name="ingestion_pandas")
            asset = datasource.add_dataframe_asset(name=f"batch_{filename}")
            batch_def = asset.add_batch_definition_whole_dataframe(name="whole")

            from great_expectations.expectations import (
                ExpectColumnToExist,
                ExpectColumnValuesToBeBetween,
                ExpectColumnValuesToBeInSet,
                ExpectColumnValuesToNotBeNull,
            )

            expectations = [ExpectColumnToExist(column=col) for col in REQUIRED_COLUMNS]
            if "number_of_customers_affected" in columns:
                expectations.append(
                    ExpectColumnValuesToNotBeNull(column="number_of_customers_affected")
                )
            if "ticket_count" in columns:
                expectations.append(
                    ExpectColumnValuesToBeBetween(column="ticket_count", min_value=0)
                )
            if "cloud_provider" in columns:
                expectations.append(
                    ExpectColumnValuesToBeInSet(
                        column="cloud_provider",
                        value_set=list(VALID_CLOUD_PROVIDERS),
                    )
                )

            suite = gx.ExpectationSuite(name="ingestion_suite")
            suite.expectations = expectations
            suite = context.suites.add_or_update(suite)

            vd_name = f"vd_{filename.replace('.', '_').replace('-', '_')}"
            try:
                vd = context.validation_definitions.get(vd_name)
                vd.suite = suite
                vd.data = batch_def
                context.validation_definitions.update(vd)
            except Exception:
                vd = context.validation_definitions.add(
                    gx.ValidationDefinition(name=vd_name, data=batch_def, suite=suite)
                )

            cp_name = f"cp_{filename.replace('.', '_').replace('-', '_')}"
            try:
                checkpoint = context.checkpoints.get(cp_name)
            except Exception:
                checkpoint = context.checkpoints.add(
                    gx.Checkpoint(name=cp_name, validation_definitions=[vd])
                )

            checkpoint.run(batch_parameters={"dataframe": df})
            context.build_data_docs()

            try:
                sites = context.get_docs_sites_urls()
                if sites:
                    report_url = sites[0].get("site_url", "")
            except Exception:
                pass

        except Exception as gx_err:
            print(f"[validate_data] GX error (non-fatal): {gx_err}")

        print(
            f"[validate_data] {filename}: total={total_rows}, "
            f"invalid={invalid_rows}, criticality={criticality}"
        )

        return {
            "filename": filename,
            "data_json": data_json,
            "total_rows": total_rows,
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "criticality": criticality,
            "error_types": error_type_counts,
            "row_errors_json": json.dumps({str(k): v for k, v in row_errors.items()}),
            "has_schema_error": has_schema_error,
            "report_url": report_url,
        }

    # -----------------------------------------------------------------------
    # Task 3a — save_statistics
    # -----------------------------------------------------------------------
    @task()
    def save_statistics(validation_result: dict) -> None:
        """Write ingestion summary to ingestion_stats table via PostgresHook."""
        pg = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg.get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO ingestion_stats
                (filename, total_rows, valid_rows, invalid_rows, criticality, error_types)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                validation_result["filename"],
                validation_result["total_rows"],
                validation_result["valid_rows"],
                validation_result["invalid_rows"],
                validation_result["criticality"],
                json.dumps(validation_result["error_types"]),
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"[save_statistics] Saved stats for {validation_result['filename']}")

    # -----------------------------------------------------------------------
    # Task 3b — send_alerts
    # -----------------------------------------------------------------------
    @task()
    def send_alerts(validation_result: dict) -> None:
        """Send Teams webhook for medium/high criticality. Skip silently for low/none."""
        criticality = validation_result["criticality"]
        if criticality not in ("medium", "high"):
            print(f"[send_alerts] criticality={criticality} — no alert needed.")
            return

        webhook_url = os.getenv(TEAMS_WEBHOOK_ENV, "")
        if not webhook_url:
            print("[send_alerts] TEAMS_WEBHOOK_URL not set — skipping.")
            return

        filename = validation_result["filename"]
        total = validation_result["total_rows"]
        invalid = validation_result["invalid_rows"]
        error_types = validation_result["error_types"]
        report_url = validation_result["report_url"]
        pct = round(invalid / total * 100, 1) if total else 0
        error_summary = ", ".join(f"{k}: {v}" for k, v in error_types.items()) or "none"

        emoji = "🔴" if criticality == "high" else "🟡"
        message = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "FF0000" if criticality == "high" else "FFA500",
            "summary": f"Data Quality Alert — {criticality.upper()}",
            "sections": [{
                "activityTitle": f"{emoji} Data Quality Alert — {criticality.upper()}",
                "activitySubtitle": f"File: `{filename}`",
                "facts": [
                    {"name": "Total Rows", "value": str(total)},
                    {"name": "Invalid Rows", "value": f"{invalid} ({pct}%)"},
                    {"name": "Error Types", "value": error_summary},
                    {"name": "Report", "value": report_url or "N/A"},
                ],
                "markdown": True,
            }],
        }

        try:
            resp = http_requests.post(webhook_url, json=message, timeout=10)
            resp.raise_for_status()
            print(f"[send_alerts] Teams alert sent — {criticality}")
        except Exception as e:
            print(f"[send_alerts] Failed: {e}")

    # -----------------------------------------------------------------------
    # Task 3c — split_and_save_data
    # -----------------------------------------------------------------------
    @task()
    def split_and_save_data(validation_result: dict) -> None:
        """Route rows: all-good → good_data, all-bad → bad_data, mixed → both."""
        os.makedirs(GOOD_DATA_DIR, exist_ok=True)
        os.makedirs(BAD_DATA_DIR, exist_ok=True)

        df = pd.read_json(validation_result["data_json"], orient="records")
        row_errors: dict[int, list] = {
            int(k): v
            for k, v in json.loads(validation_result["row_errors_json"]).items()
        }
        filename = validation_result["filename"]
        total_rows = validation_result["total_rows"]
        invalid_rows = validation_result["invalid_rows"]

        bad_idx = set(row_errors.keys())
        good_df = df[~df.index.isin(bad_idx)]
        bad_df = df[df.index.isin(bad_idx)]

        if invalid_rows == 0:
            df.to_csv(os.path.join(GOOD_DATA_DIR, filename), index=False)
            print(f"[split] All good → good_data/{filename}")
        elif invalid_rows == total_rows:
            df.to_csv(os.path.join(BAD_DATA_DIR, filename), index=False)
            print(f"[split] All bad → bad_data/{filename}")
        else:
            base, ext = os.path.splitext(filename)
            good_df.to_csv(os.path.join(GOOD_DATA_DIR, f"{base}_good{ext}"), index=False)
            bad_df.to_csv(os.path.join(BAD_DATA_DIR, f"{base}_bad{ext}"), index=False)
            print(f"[split] Mixed: {len(good_df)} good, {len(bad_df)} bad")

    # ── Wiring ───────────────────────────────────────────────────────────────
    file_info = read_data()
    validation_result = validate_data(file_info)

    # Parallel downstream tasks
    save_statistics(validation_result)
    send_alerts(validation_result)
    split_and_save_data(validation_result)


ingestion_dag()
