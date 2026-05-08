"""
generate_data_issues.py — Inject data quality errors into a clean CSV dataset.

Implements the required 5 categories + 2 additional error types (7 total).

Usage:
    python generate_data_issues.py --input data/cloud_outages_dataset.csv \\
        --output data/raw_data/corrupted.csv --probability 0.1
"""
import pandas as pd
import numpy as np
import argparse
import os


def inject_errors(df: pd.DataFrame, probability: float) -> pd.DataFrame:
    df = df.copy()
    n = len(df)
    rng = np.random.default_rng(seed=42)

    def mask(prob=None):
        return rng.random(n) < (prob or probability)

    # --- Required 5 error types ---

    # 1. Completeness — Null values in required column
    null_mask = mask()
    df.loc[null_mask, "number_of_customers_affected"] = np.nan
    print(f"[1] Null values injected in 'number_of_customers_affected': {null_mask.sum()} rows")

    # 2. Validity — Value outside valid range (negative ticket_count)
    range_mask = mask()
    df.loc[range_mask, "ticket_count"] = rng.integers(-500, -1, size=range_mask.sum())
    print(f"[2] Out-of-range values injected in 'ticket_count': {range_mask.sum()} rows")

    # 3. Consistency — Invalid categorical value
    cat_mask = mask()
    df.loc[cat_mask, "cloud_provider"] = "INVALID_CLOUD"
    print(f"[3] Invalid categorical values in 'cloud_provider': {cat_mask.sum()} rows")

    # 4. Schema — Missing required column (applied at file level with probability)
    if rng.random() < probability:
        df = df.drop(columns=["severity"], errors="ignore")
        print("[4] Schema error: 'severity' column removed from file")
    else:
        print("[4] Schema error: skipped (not triggered this run)")

    # 5. Type — Wrong data type in numeric column
    type_mask = mask()
    df.loc[type_mask, "system_load_before_outage"] = "NOT_A_NUMBER"
    print(f"[5] Wrong data type in 'system_load_before_outage': {type_mask.sum()} rows")

    # --- Additional error types ---

    # 6. Duplicate primary keys
    dup_mask = mask(probability / 2)
    dup_indices = df.index[dup_mask].tolist()
    if dup_indices:
        duplicates = df.loc[dup_indices].copy()
        df = pd.concat([df, duplicates], ignore_index=True)
        print(f"[6] Duplicate rows injected: {len(dup_indices)} duplicates added")
    else:
        print("[6] Duplicate rows: skipped (not triggered this run)")

    # 7. Statistical outliers — extreme values
    # Recreate RNG and mask with updated dataframe size
    n_updated = len(df)
    outlier_mask = rng.random(n_updated) < (probability / 2)
    if "duration_minutes" in df.columns:
        df.loc[outlier_mask, "duration_minutes"] = 9_999_999
        print(f"[7] Statistical outliers in 'duration_minutes': {outlier_mask.sum()} rows")

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inject data quality errors into a clean dataset")
    parser.add_argument("--input", required=True, help="Path to clean CSV input file")
    parser.add_argument("--output", required=True, help="Path to save the corrupted CSV")
    parser.add_argument("--probability", type=float, default=0.1,
                        help="Error injection probability per row (0.0 - 1.0)")
    args = parser.parse_args()

    print(f"\nReading: {args.input}")
    df = pd.read_csv(args.input)
    print(f"Dataset shape: {df.shape}\n")

    corrupted = inject_errors(df, args.probability)

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    corrupted.to_csv(args.output, index=False)
    print(f"\n✅ Corrupted dataset saved to: {args.output}")
    print(f"   Original rows: {len(df)}, Output rows: {len(corrupted)}")
