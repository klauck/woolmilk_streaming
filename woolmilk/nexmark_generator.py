import argparse
import json
import subprocess
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

def write_parquet(filepath: str, overall_tuple: int, rows_per_group:int, table: pa.Table):

    output_file = Path(filepath)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Created File: {output_file}")

    start = time.time()
    pq.write_table(
        table,
        output_file,
        row_group_size=rows_per_group,
        compression="snappy")

    duration = time.time() - start
    print(f"[INFO] Populated File in {duration:.2f}s with Data:")
    print(f"\t {rows_per_group} rows per Group, Total {overall_tuple} tuples")
    size_mb = output_file.stat().st_size / (1000**2)
    print(f"[INFO] File Size (est.): {size_mb:.2f} MB")

def generate_data(stream_type: str, overall_tuples: int, generator_exec: str) -> pa.Table:
    cmd = [
        generator_exec,
        "--type", stream_type,
        "-n", str(overall_tuples),
        "--no-wait",
    ]

    print(f"[INFO] Starting Nexmark generator:\n       {' '.join(cmd)}")
    start = time.time()


    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    records = []

    try:
        for line in proc.stdout:
            try:
                record = json.loads(line)
                key = stream_type.capitalize()
                records.append(record[key])
            except json.JSONDecodeError:
                continue
    finally:
        proc.stdout.close()
        proc.kill()
        proc.wait()

    duration = time.time() - start
    print(f"[INFO] Generated {len(records)} {stream_type} events ({overall_tuples} tuples) [{duration:.2f}s]")

    start = time.time()
    table = pa.Table.from_pylist(records)
    table = table.drop_columns(["date_time"])
    duration = time.time() - start
    print(f"[INFO] Arrow Table created with {table.num_rows} rows. [{duration:.2f}s]")

    return table

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Nexmark TestData Parquet File")
    parser.add_argument(
        "--stream",
        choices=["bid", "auction", "person"],
        default="person",
        help="Stream type",
    )

    parser.add_argument(
        "--generator-executable", default="nexmark", help="Executable to generate data"
    )

    parser.add_argument(
        "--rows-per-group",
        type=int,
        help="Number of Rows per RowGroup",
        default=10 ** 5,
    )

    parser.add_argument(
        "--overall-tuples",
        type=int,
        help="Number of Tuples in total",
        default=10 ** 6,
    )

    parser.add_argument(
        "--filepath",
        type=str,
        help="File to write to",
        default="data/test.parquet",
    )

    args: argparse.Namespace = parser.parse_args()

    print("\n" + "=" * 40)
    print(" Nexmark to Parquet Generator")
    print("=" * 40)
    print(f" Stream Type                : {args.stream}")
    print(f" Rows per Group             : {args.rows_per_group}")
    print(f" Tuples in total            : {args.overall_tuples}")
    print(f" Filepath                   : {args.filepath}")
    print("=" * 40 + "\n")

    return args


if __name__ == "__main__":
    args = parse_arguments()
    table: pa.Table = generate_data(args.stream, args.overall_tuples, args.generator_executable)
    write_parquet(args.filepath, args.overall_tuples, args.rows_per_group, table)