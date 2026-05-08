"""
prediction_dag.py — Scheduled Prediction DAG (Airflow 3.x)

Runs every 2 minutes.

Task 1 — check_for_new_data:
    Scan good_data/ for files newer than the last processed timestamp.
    If none → raise AirflowSkipException (marks the entire DAG run as skipped).
    Otherwise → return list of new file paths via XCom.

Task 2 — make_predictions:
    Read each new file and POST to /predict (source="scheduled").
    API saves predictions to the database.
"""

from __future__ import annotations

import glob
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests as http_requests

# Airflow 3: imports consolidated under airflow.sdk
from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GOOD_DATA_DIR = "/opt/airflow/data/good_data"
TRACKER_FILE = "/opt/airflow/data/.last_prediction_timestamp"
API_URL = os.getenv("API_URL", "http://api:8000")

REQUIRED_COLS = [
    "cloud_provider", "service", "severity",
    "number_of_customers_affected", "ticket_count", "backup_system_triggered",
]


def _read_last_ts() -> float:
    """Return the epoch timestamp of the last processed file, or 0.0."""
    try:
        if os.path.exists(TRACKER_FILE):
            return float(Path(TRACKER_FILE).read_text().strip())
    except Exception:
        pass
    return 0.0


def _write_last_ts(ts: float) -> None:
    Path(TRACKER_FILE).write_text(str(ts))


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id="prediction_dag",
    schedule="*/2 * * * *",          # every 2 minutes
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["prediction", "scheduled"],
)
def prediction_dag():

    # -----------------------------------------------------------------------
    # Task 1 — check_for_new_data
    # -----------------------------------------------------------------------
    @task()
    def check_for_new_data() -> list[str]:
        """
        Check good_data/ for CSV files newer than the last processed timestamp.
        Raises AirflowSkipException to skip the entire DAG run if no new data.
        """
        last_ts = _read_last_ts()
        all_files = glob.glob(os.path.join(GOOD_DATA_DIR, "*.csv"))
        new_files = [f for f in all_files if os.path.getmtime(f) > last_ts]

        print(f"[check_for_new_data] last_ts={last_ts}, found {len(new_files)} new file(s)")

        if not new_files:
            # Airflow 3: raising AirflowSkipException in the first task causes
            # all downstream tasks to be skipped and the DAG run shows as skipped.
            raise AirflowSkipException(
                f"No new files in {GOOD_DATA_DIR} since last run — skipping DAG run."
            )

        return new_files

    # -----------------------------------------------------------------------
    # Task 2 — make_predictions
    # -----------------------------------------------------------------------
    @task()
    def make_predictions(new_files: list[str]) -> None:
        """
        Read each new validated file and call POST /predict.
        The API saves predictions to the database with source='scheduled'.
        """
        max_ts = _read_last_ts()

        for fpath in new_files:
            try:
                df = pd.read_csv(fpath)

                # Ensure all required columns are present
                available = [c for c in REQUIRED_COLS if c in df.columns]
                if len(available) < len(REQUIRED_COLS):
                    missing = set(REQUIRED_COLS) - set(available)
                    print(f"[make_predictions] Skipping {fpath}: missing cols {missing}")
                    continue

                df_infer = df[available].dropna(subset=available)
                if df_infer.empty:
                    print(f"[make_predictions] No valid rows in {fpath}")
                    continue

                payload = {"features": df_infer.to_dict(orient="records"), "source": "scheduled"}

                resp = http_requests.post(f"{API_URL}/predict", json=payload, timeout=60)
                resp.raise_for_status()

                preds = resp.json().get("predictions", [])
                print(
                    f"[make_predictions] {os.path.basename(fpath)}: "
                    f"{len(preds)} predictions saved."
                )

                file_ts = os.path.getmtime(fpath)
                if file_ts > max_ts:
                    max_ts = file_ts

            except Exception as e:
                print(f"[make_predictions] Error on {fpath}: {e}")

        _write_last_ts(max_ts)
        print(f"[make_predictions] Done. Updated last_ts → {max_ts}")

    # ── Wiring ───────────────────────────────────────────────────────────────
    new_files = check_for_new_data()
    make_predictions(new_files)


prediction_dag()
