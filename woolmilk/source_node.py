import argparse
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet as pq


def generate_table(
    number_of_tuples=10**6,
    stream="nexmark_person",
    generator_executable="nexmark",
    offset=0,
    step=1,
):
    assert len(stream.split("_")) == 2 and stream.split("_")[0] == "nexmark"
    event_type = stream.split("_")[1]
    cmd = [
        generator_executable,
        "-n",
        str(number_of_tuples),
        "--offset",
        str(offset),
        "--step",
        str(step),
        "--type",
        event_type,
        "--no-wait",
    ]
    print("Generate data..")
    print(
        f"  {event_type} \t number of tuples: {number_of_tuples} \t offset: {offset} \t step: {step}"
    )
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    records = []

    try:
        for line in proc.stdout:
            try:
                record = json.loads(line)
                key = event_type.capitalize()
                records.append(record[key])
            except json.JSONDecodeError:
                continue
    finally:
        proc.stdout.close()
        proc.kill()
        proc.wait()

    print("Done.")
    return pa.Table.from_pylist(records)


def stream_data(
    client_id,
    input_folder,
    stream,
    generator_executable,
    offset,
    step,
    processing_node,
    number_of_tuples,
    tuples_per_batch,
    store_input,
):
    # generate (cached) Parquet file for input
    path = Path(input_folder) / f"{stream}_{number_of_tuples}_{offset}_{step}.parquet"
    if not path.exists():
        table = generate_table(
            number_of_tuples, stream, generator_executable, offset, step
        )
        if store_input:
            path.parent.mkdir(parents=True, exist_ok=True)
            print(f"Wrote .. {path}")
            pq.write_table(
                table, path, row_group_size=tuples_per_batch, compression="snappy"
            )
        schema = table.schema
    else:
        parquet_file = pq.ParquetFile(path)
        schema = parquet_file.schema_arrow
        table = parquet_file.read()

    # table = table.drop_columns(["date_time"])
    # schema = schema.append(pa.field("timestamp", pa.int64()))

    client = pa.flight.FlightClient(f"grpc://{processing_node[0]}:{processing_node[1]}")
    writer, _ = client.do_put(
        pa.flight.FlightDescriptor.for_path("bandwidth-test"), schema
    )

    start = time.time()
    send_times = []
    total_bytes = 0
    for i, batch in enumerate(table.to_batches(max_chunksize=args.tuples_per_batch)):
        send_start = time.time()
        writer.write_batch(batch)
        total_bytes += batch.nbytes
        send_end = time.time()
        send_times.append((send_start, send_end))

    writer.done_writing()

    end = time.time()

    duration = end - start
    gbps = (total_bytes * 8) / (duration * 1000**3)
    mbps = total_bytes / (duration * 1000**2)

    print(f"[Client {client_id}] Start: {start}")
    print("send_times = ", send_times)
    print(
        f"{client_id}: Sent {total_bytes / 1000 ** 2} MB in {duration:.7f} seconds; "
        f"{gbps:.4f} Gbps ({mbps:.2f} MBps)"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Source Node")
    parser.add_argument(
        "--input-folder",
        default="input_data",
        help="Input Parquet files or folder to store generated data",
    )
    parser.add_argument(
        "--stream",
        choices=["nexmark_bid", "nexmark_auction", "nexmark_person"],
        default="nexmark_person",
        help="Stream type",
    )
    parser.add_argument(
        "--generator-executable", default="nexmark", help="Executable to generate data"
    )
    parser.add_argument(
        "--overall-tuples",
        type=int,
        default=10**5,
        help="Total number of tuples needs to be sent.",
    )
    parser.add_argument(
        "--tuples-per-batch", type=int, default=10**4, help="Number of tuples per batch"
    )
    parser.add_argument(
        "--offset", type=int, default=0, help="Offset to start data generation"
    )
    parser.add_argument(
        "--step", type=int, default=-1, help="Step for next tuple to generate"
    )
    parser.add_argument(
        "--processing-nodes",
        help="Flight server address (host:port,host:port)",
        type=str,
        default="localhost:8010",
    )
    parser.add_argument(
        "-store-input",
        action="store_true",
        help="Store generated data",
        default="",
    )
    args = parser.parse_args()

    processing_nodes = []
    for address in args.processing_nodes.split(","):
        host, port = address.split(":")
        processing_nodes.append((host, int(port)))

    if args.step == -1:
        args.step = len(processing_nodes)

    print("\n" + "=" * 40)
    print(" WoolMilk Source Node Parameters")
    print("=" * 40)
    print(f" Input Folder               : {args.input_folder}")
    print(f" Stream Type                : {args.stream}")
    print(f" Overall Tuples             : {args.overall_tuples}")
    print(f" Tuples Per Batch           : {args.tuples_per_batch}")
    print(f" Offset                     : {args.offset}")
    print(f" Step                       : {args.step}")
    print(f" Store Input                : {args.store_input}")
    print(f" Processing Nodes           : {args.processing_nodes}")
    print(f" Generator Executable       : {args.generator_executable}")
    print("=" * 40 + "\n")

    if len(processing_nodes) == 0:
        print("No server addresses for processing nodes provided. Exiting.")
        sys.exit(1)

    assert args.overall_tuples % (args.tuples_per_batch * len(processing_nodes)) == 0

    threads = []
    for client_id in range(len(processing_nodes)):
        t = threading.Thread(
            target=stream_data,
            args=(
                client_id,
                args.input_folder,
                args.stream,
                args.generator_executable,
                args.offset + client_id,
                args.step,
                processing_nodes[client_id],
                args.overall_tuples // len(processing_nodes),
                args.tuples_per_batch,
                args.store_input,
            ),
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
