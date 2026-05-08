"""
split_dataset.py — Split a CSV dataset into N files in raw_data/

Usage:
    python split_dataset.py --input data/cloud_outages_dataset.csv --output data/raw_data --num-files 30
"""
import pandas as pd
import os
import argparse


def split_dataset(input_path: str, output_dir: str, num_files: int):
    df = pd.read_csv(input_path)
    os.makedirs(output_dir, exist_ok=True)

    chunks = [df[i::num_files] for i in range(num_files)]

    for i, chunk in enumerate(chunks, start=1):
        out_path = os.path.join(output_dir, f"batch_{i:03d}.csv")
        chunk.to_csv(out_path, index=False)
        print(f" Saved {out_path} ({len(chunk)} rows)")

    print(f"\nDone! {num_files} files saved to '{output_dir}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split dataset into N files")
    parser.add_argument("--input", required=True, help="Path to input CSV")
    parser.add_argument("--output", default="data/raw_data", help="Output directory")
    parser.add_argument("--num-files", type=int, required=True, help="Number of files to generate")
    args = parser.parse_args()

    split_dataset(args.input, args.output, args.num_files)
