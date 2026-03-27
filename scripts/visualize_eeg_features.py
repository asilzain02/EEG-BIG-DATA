#!/usr/bin/env python3
"""
Read Hadoop MapReduce output for EEG features and plot simple interpretation charts.

Usage:
  python scripts/visualize_eeg_features.py --input eeg_features_output.txt --output-dir plots
"""

import argparse
import os
import matplotlib.pyplot as plt


def parse_output(file_path: str):
    """Parse line: GLOBAL\tcount,mean,min,max,stddev"""
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if "\t" not in line:
                continue
            key, vals = line.split("\t", 1)
            if key != "GLOBAL":
                continue
            parts = vals.split(",")
            if len(parts) != 5:
                continue
            count, mean, min_v, max_v, stddev = parts
            return {
                "count": int(count),
                "mean": float(mean),
                "min": float(min_v),
                "max": float(max_v),
                "stddev": float(stddev),
            }
    raise ValueError("No valid GLOBAL output found in file")


def plot_features(features, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    labels = ["Mean", "Min", "Max", "Std Dev"]
    values = [features["mean"], features["min"], features["max"], features["stddev"]]

    plt.figure(figsize=(8, 5))
    bars = plt.bar(labels, values)
    plt.title("EEG Global Signal Features")
    plt.ylabel("Value")

    for bar, val in zip(bars, values):
        plt.text(bar.get_x() + bar.get_width() / 2, val, f"{val:.2f}", ha="center", va="bottom")

    out_path = os.path.join(output_dir, "eeg_global_features.png")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"Saved plot: {out_path}")
    print(f"Total valid samples: {features['count']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Hadoop output text file (downloaded part-r-00000)")
    parser.add_argument("--output-dir", default="plots", help="Directory to save charts")
    args = parser.parse_args()

    features = parse_output(args.input)
    print("Parsed features:", features)
    plot_features(features, args.output_dir)


if __name__ == "__main__":
    main()
