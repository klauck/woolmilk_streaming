import argparse
import os
import sys

import matplotlib.pyplot as plt

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.benchmark_windowing import run_in_subprocess


def main():
    parser = argparse.ArgumentParser(description="Plot windowing sweep for records")
    parser.add_argument("--start-records", type=int, default=100000)
    parser.add_argument("--step-records", type=int, default=100000)
    parser.add_argument("--steps", type=int, default=10)
    parser.add_argument("--window-size", type=int, default=10, help="Window size in seconds")
    parser.add_argument("--window-slide", type=int, default=2, help="Window slide in seconds")
    parser.add_argument("--output", type=str, default="windowing_sweep.png")
    args = parser.parse_args()

    records_list = [
        args.start_records + i * args.step_records for i in range(args.steps)
    ]

    peak_rss_mb = []
    processing_seconds = []
    windows_list = []
    input_mb = []

    for records in records_list:
        result = run_in_subprocess(
            records,
            10000,
            args.window_size,
            args.window_slide,
        )
        peak_rss_mb.append(result["peak_rss_bytes"] / (1024 * 1024))
        processing_seconds.append(result["processing_seconds"])
        windows_list.append(result["windows"])
        input_mb.append(result["input_bytes"] / (1024 * 1024))

    fig, ax1 = plt.subplots(figsize=(8, 5))
    ax2 = ax1.twinx()

    ax1.plot(records_list, peak_rss_mb, marker="o", label="Peak RSS (MB)")
    ax2.plot(records_list, processing_seconds, marker="s", color="tab:orange", label="Processing time (s)")

    for x, w, mb, y in zip(records_list, windows_list, input_mb, peak_rss_mb):
        ax1.annotate(
            f"windows={w}, input={mb:.1f}MB",
            (x, y),
            textcoords="offset points",
            xytext=(0, 6),
            ha="center",
            fontsize=8,
        )

    ax1.set_xlabel("Number of Records")
    ax1.set_ylabel("Peak RSS (MB)")
    ax2.set_ylabel("Processing time (s)")
    ax1.grid(True, alpha=0.4)
    ax1.set_title("Peak RSS and Processing Time")

    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper left")

    fig.tight_layout()
    fig.savefig(args.output, dpi=150)
    print(f"Saved plot to {args.output}")


if __name__ == "__main__":
    main()
