#!/usr/bin/env python3
"""
Convert EEG EDF files to CSV so they can be ingested by Hadoop MapReduce.

Output CSV schema:
    timestamp,channel,value

Example:
    python scripts/edf_to_csv.py \
      --input-dir /home/hadoop/eeg_edf \
      --output-dir /home/hadoop/eeg_csv \
      --step 1
"""

import argparse
import csv
import glob
import os
from pathlib import Path

import pyedflib


def convert_edf_file(input_path: str, output_path: str, step: int = 1) -> None:
    """Convert one EDF file to long-format CSV."""
    reader = pyedflib.EdfReader(input_path)
    try:
        n_channels = reader.signals_in_file
        labels = reader.getSignalLabels()

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "channel", "value"])

            for ch_idx in range(n_channels):
                signal = reader.readSignal(ch_idx)
                fs = float(reader.getSampleFrequency(ch_idx))
                label = labels[ch_idx]

                for i in range(0, len(signal), step):
                    timestamp = i / fs
                    writer.writerow([f"{timestamp:.6f}", label, f"{float(signal[i]):.10f}"])
    finally:
        reader.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert EDF EEG files to CSV for Hadoop jobs")
    parser.add_argument("--input-dir", required=True, help="Directory containing .edf files")
    parser.add_argument("--output-dir", required=True, help="Directory to write .csv files")
    parser.add_argument(
        "--step",
        type=int,
        default=1,
        help="Sampling step for downsampling while exporting (1 = keep all points)",
    )
    args = parser.parse_args()

    if args.step < 1:
        raise ValueError("--step must be >= 1")

    os.makedirs(args.output_dir, exist_ok=True)
    edf_files = sorted(glob.glob(os.path.join(args.input_dir, "*.edf")))

    if not edf_files:
        raise FileNotFoundError(f"No .edf files found in {args.input_dir}")

    for edf_file in edf_files:
        base_name = Path(edf_file).stem
        out_csv = os.path.join(args.output_dir, f"{base_name}.csv")
        print(f"Converting: {edf_file} -> {out_csv}")
        convert_edf_file(edf_file, out_csv, step=args.step)

    print(f"Done. Converted {len(edf_files)} EDF files.")


if __name__ == "__main__":
    main()
