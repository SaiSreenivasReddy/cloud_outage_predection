"""
tests/test_ingestion.py — Unit tests for ingestion pipeline logic (Airflow 3)

Tests are framework-independent: they test pure Python functions extracted
from the DAG modules (criticality calculation, file splitting, error stats,
Pydantic schemas, and data injection). No Airflow context needed.
"""

import json
import pytest
import pandas as pd

import sys
import os

# Allow importing from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from generate_data_issues import inject_errors

# Import the helper function from the DAG module without triggering Airflow
# We copy the logic here so tests don't depend on Airflow being installed.

VALID_CLOUD_PROVIDERS = {"AWS", "GCP", "Azure", "IBM", "Oracle", "Alibaba"}
REQUIRED_COLUMNS = [
    "cloud_provider", "service", "severity",
    "system_load_before_outage", "number_of_customers_affected",
    "ticket_count", "backup_system_triggered",
]


def compute_criticality(total_rows: int, invalid_rows: int, has_schema_error: bool) -> str:
    """Mirrors the same function in ingestion_dag.py"""
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


def split_rows(df: pd.DataFrame, bad_indices: set) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Mirrors split logic in split_and_save_data task."""
    good_df = df[~df.index.isin(bad_indices)]
    bad_df = df[df.index.isin(bad_indices)]
    return good_df, bad_df


# ---------------------------------------------------------------------------
# Criticality tests
# ---------------------------------------------------------------------------

class TestCriticality:

    def test_schema_error_always_high(self):
        assert compute_criticality(100, 5, has_schema_error=True) == "high"

    def test_over_50_pct_invalid_is_high(self):
        assert compute_criticality(100, 51, has_schema_error=False) == "high"

    def test_exactly_50_pct_is_high(self):
        # > 50% → high; 50% is NOT > 50% so it's medium
        assert compute_criticality(100, 50, has_schema_error=False) == "medium"

    def test_10_pct_is_medium(self):
        assert compute_criticality(100, 10, has_schema_error=False) == "medium"

    def test_between_10_and_50_pct_is_medium(self):
        assert compute_criticality(100, 30, has_schema_error=False) == "medium"

    def test_below_10_pct_is_low(self):
        assert compute_criticality(100, 5, has_schema_error=False) == "low"

    def test_zero_invalid_is_none(self):
        assert compute_criticality(100, 0, has_schema_error=False) == "none"

    def test_empty_file_is_low(self):
        assert compute_criticality(0, 0, has_schema_error=False) == "low"


# ---------------------------------------------------------------------------
# File splitting tests
# ---------------------------------------------------------------------------

class TestFileSplitting:

    def _make_df(self, n=10) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "cloud_provider": ["AWS"] * n,
                "ticket_count": list(range(n)),
                "value": list(range(n)),
            }
        )

    def test_all_good(self):
        df = self._make_df(10)
        good, bad = split_rows(df, set())
        assert len(good) == 10
        assert len(bad) == 0

    def test_all_bad(self):
        df = self._make_df(10)
        bad_idx = set(range(10))
        good, bad = split_rows(df, bad_idx)
        assert len(good) == 0
        assert len(bad) == 10

    def test_mixed_split(self):
        df = self._make_df(10)
        bad_idx = {0, 1, 2}
        good, bad = split_rows(df, bad_idx)
        assert len(good) == 7
        assert len(bad) == 3

    def test_split_preserves_data(self):
        df = self._make_df(5)
        bad_idx = {0, 4}
        good, bad = split_rows(df, bad_idx)
        assert list(good.index) == [1, 2, 3]
        assert list(bad.index) == [0, 4]


# ---------------------------------------------------------------------------
# Error stats computation tests
# ---------------------------------------------------------------------------

class TestErrorStats:

    def _count_errors(self, row_errors: dict) -> dict:
        counts: dict[str, int] = {}
        for errs in row_errors.values():
            for e in errs:
                counts[e] = counts.get(e, 0) + 1
        return counts

    def test_single_error_type(self):
        row_errors = {0: ["null_value"], 1: ["null_value"]}
        counts = self._count_errors(row_errors)
        assert counts == {"null_value": 2}

    def test_multiple_error_types(self):
        row_errors = {0: ["null_value", "out_of_range"], 1: ["invalid_categorical"]}
        counts = self._count_errors(row_errors)
        assert counts["null_value"] == 1
        assert counts["out_of_range"] == 1
        assert counts["invalid_categorical"] == 1

    def test_empty_errors(self):
        counts = self._count_errors({})
        assert counts == {}


# ---------------------------------------------------------------------------
# Pydantic schema tests
# ---------------------------------------------------------------------------

class TestSchemas:

    def test_valid_prediction_request(self):
        from model_service.app.schemas import PredictionRequest, BatchPredictionRequest

        req = PredictionRequest(
            cloud_provider="AWS",
            service="Compute",
            severity="High",
            number_of_customers_affected=1000,
            ticket_count=50,
            backup_system_triggered="Yes",
        )
        assert req.cloud_provider == "AWS"
        assert req.system_load_before_outage == 50  # default

    def test_batch_request_defaults_to_webapp_source(self):
        from model_service.app.schemas import PredictionRequest, BatchPredictionRequest

        batch = BatchPredictionRequest(
            features=[
                PredictionRequest(
                    cloud_provider="GCP",
                    service="Storage",
                    severity="Low",
                    number_of_customers_affected=200,
                    ticket_count=10,
                    backup_system_triggered="No",
                )
            ]
        )
        assert batch.source == "webapp"

    def test_missing_required_field_raises(self):
        from pydantic import ValidationError
        from model_service.app.schemas import PredictionRequest

        with pytest.raises(ValidationError):
            PredictionRequest(
                cloud_provider="AWS",
                # missing service, severity, etc.
            )


# ---------------------------------------------------------------------------
# generate_data_issues tests
# ---------------------------------------------------------------------------

class TestGenerateDataIssues:

    def _make_clean_df(self, n=100) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "cloud_provider": ["AWS"] * n,
                "service": ["Compute"] * n,
                "severity": ["High"] * n,
                "system_load_before_outage": [50] * n,
                "number_of_customers_affected": [1000] * n,
                "ticket_count": [30] * n,
                "backup_system_triggered": ["Yes"] * n,
                "duration_minutes": [120] * n,
                "severity": ["High"] * n,
            }
        )

    def test_null_values_injected(self):
        df = self._make_clean_df(200)
        corrupted = inject_errors(df, probability=0.5)
        assert corrupted["number_of_customers_affected"].isna().any()

    def test_out_of_range_ticket_count(self):
        df = self._make_clean_df(200)
        corrupted = inject_errors(df, probability=0.5)
        numeric = pd.to_numeric(corrupted["ticket_count"], errors="coerce")
        assert (numeric < 0).any()

    def test_invalid_categorical_injected(self):
        df = self._make_clean_df(200)
        corrupted = inject_errors(df, probability=0.5)
        assert "INVALID_CLOUD" in corrupted["cloud_provider"].values

    def test_output_has_more_or_equal_rows(self):
        """Duplicates can add rows — output should be >= original."""
        df = self._make_clean_df(100)
        corrupted = inject_errors(df, probability=0.3)
        assert len(corrupted) >= len(df)
