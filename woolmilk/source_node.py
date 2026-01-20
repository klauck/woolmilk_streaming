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
    num_rows=10**6, event_type="person", generator_executable="nexmark", offset=0, step=1
):
    cmd = [
        generator_executable,
        "-n",
        str(num_rows),
        "--offset",
        str(offset),
        "--step",
        str(step),
        "--type",
        event_type,
        "--no-wait",
    ]
    print("Generate data..")
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


def send_data(
    thread_id,
    schema,
    batches,
    processing_nodes,
    experiment_id=None,
    iteration_id=None,
    source_node_id=None,
):
    path_info = {
        "path": "bandwidth-test",
        "experiment_id": experiment_id,
        "iteration_id": iteration_id,
        "source_node_id": source_node_id,
        "thread_id": thread_id,
    }
    encoded_path = json.dumps(path_info)

    writers = []
    for processing_node in processing_nodes:
        client = pa.flight.FlightClient(
            f"grpc://{processing_node[0]}:{processing_node[1]}"
        )
        writer, _ = client.do_put(
            pa.flight.FlightDescriptor.for_path(encoded_path), schema
        )
        writers.append(writer)

    start = time.time()
    send_times = []
    total_bytes = 0
    for i, batch in enumerate(batches):
        send_start = time.time()
        writers[(thread_id + i) % len(processing_nodes)].write_batch(batch)
        total_bytes += batch.nbytes
        send_end = time.time()
        send_times.append((send_start, send_end))

    for writer in writers:
        writer.done_writing()

    end = time.time()

    duration = end - start
    gbps = (total_bytes * 8) / (duration * 1000**3)
    mbps = total_bytes / (duration * 1000**2)

    log = {
        "thread": thread_id,
        "experiment_id": experiment_id,
        "iteration_id": iteration_id,
        "source_node_id": source_node_id,
        "send_times": send_times,
        "total_bytes": total_bytes,
        "start_time": start,
        "end_time": end,
        "gbps": f"{gbps:.4f}",
        "mbps": f"{mbps:.2f}",
    }

    print(f'WM_LOG= {json.dumps(log)}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Source Node")
    parser.add_argument(
        "--stream",
        choices=["nexmark.bid", "nexmark.auction", "nexmark.person"],
        default="nexmark.person",
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
        "--step", type=int, default=1, help="Step for next tuple to generate"
    )
    parser.add_argument(
        "--processing-nodes",
        help="Flight server address (host:port,host:port)",
        type=str,
        default="localhost:8010",
    )
    parser.add_argument(
        "--thread-count",
        type=int,
        help="Number of threads to use for sending data",
        default=1,
    )
    parser.add_argument(
        "--store-input",
        type=str,
        help="Folder to store generated data",
        default="",
    )
    parser.add_argument(
        "--experiment-id",
        type=str,
        default=None,
        help="Experiment ID for logging metadata",
    )
    parser.add_argument(
        "--iteration-id",
        type=str,
        default=None,
        help="Iteration ID for logging metadata",
    )
    parser.add_argument(
        "--source-node-id",
        type=str,
        default=None,
        help="Unique ID for the source node",
    )
    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Source Node Parameters")
    print("=" * 40)
    print(f" Stream Type                : {args.stream}")
    print(f" Overall Tuples             : {args.overall_tuples}")
    print(f" Tuples Per Batch           : {args.tuples_per_batch}")
    print(f" Offset                     : {args.offset}")
    print(f" Step                       : {args.step}")
    print(f" Processing Nodes           : {args.processing_nodes}")
    print(f" Thread Count               : {args.thread_count}")
    print(f" Store Input                : {args.store_input}")
    print(f" Generator Executable       : {args.generator_executable}")
    print(f" Experiment ID              : {args.experiment_id}")
    print(f" Iteration ID               : {args.iteration_id}")
    print(f" Source Node ID             : {args.source_node_id}")
    print("=" * 40 + "\n")

    processing_nodes = []
    for address in args.processing_nodes.split(","):
        host, port = address.split(":")
        processing_nodes.append((host, int(port)))

    if len(processing_nodes) == 0:
        print("No server addresses for processing nodes provided. Exiting.")
        sys.exit(1)

    event_type = args.stream.split(".")[1]

    table = generate_table(
        num_rows=args.overall_tuples,
        event_type=event_type,
        generator_executable=args.generator_executable,
        offset=args.offset,
        step=args.step,
    )
    batches = table.to_batches(max_chunksize=args.tuples_per_batch)

    if args.store_input != "":
        input_file = Path(args.store_input)
        input_folder = input_file.parent
        os.makedirs(input_folder, exist_ok=True)
        table = pa.Table.from_batches(batches)
        pq.write_table(table, f"{input_file}")
        print(f"Wrote .. {input_file}")

    threads = []
    for thread_id in range(args.thread_count):
        t = threading.Thread(
            target=send_data,
            args=(
                thread_id,
                table.schema,
                batches[thread_id :: args.thread_count],
                processing_nodes,
                args.experiment_id,
                args.iteration_id,
                args.source_node_id,
            ),
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
