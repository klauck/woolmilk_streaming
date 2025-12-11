import argparse
import json
import os
import subprocess
import threading
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as pq

STOP_FLAG = threading.Event()

def generate_table(num_rows=10**6, event_type="person", generator_executable="nexmark", offset=0, step=1) -> pa.Table:
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

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Woolmilk Source node')
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
        default=10 ** 6,
        help="Total number of tuples needs to be sent.",
    )
    parser.add_argument(
        "--tuples-per-batch", type=int, default=10 ** 5, help="Number of tuples per batch"
    )
    parser.add_argument(
        "--timer", type=float, default=1.0, help="Waiting time after each send of batch"
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
    args: argparse.Namespace = parser.parse_args()

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
    print("=" * 40 + "\n")
    return args

def store_input(table: pa.Table, location: str):
    input_file = Path(location)
    input_folder = input_file.parent
    os.makedirs(input_folder, exist_ok=True)
    pq.write_table(table, f"{input_file}")
    print(f"Wrote .. {input_file}")

def parse_nodes(nodes_str: str) -> list[tuple[str, int]]:
    nodes = []
    for entry in nodes_str.split(","):
        host, port = entry.split(":")
        nodes.append((host.strip(), int(port)))
    return nodes

def connect_to_processing_nodes(nodes: list[tuple[str, int]], schema: pa.Schema) -> list[tuple[str, flight.FlightStreamWriter]]:
    clients: list[tuple[str, flight.FlightStreamWriter]] = []

    for host, port in nodes:
        address = f"grpc://{host}:{port}"
        client = flight.FlightClient(address)
        writer, _ = client.do_put(
            flight.FlightDescriptor.for_path("bandwidth-test"),
            schema
        )
        clients.append((address, writer))

        print(f"Connected to {address}")

    return clients

def start_multi_thread(clients:  list[tuple[str, flight.FlightStreamWriter]], batches: list[pa.RecordBatch], wait: float, thread_count: int):
    threads = []
    for thread_id in range(thread_count):
        for addr, writer in clients:
            t = threading.Thread(
                target=send_batch,
                args=(thread_id, addr, writer, batches, wait),
                daemon=False
            )
            t.start()
            threads.append(t)

    return threads

def send_batch(thread_id: int, addr: str, writer: flight.FlightStreamWriter, batches: list[pa.RecordBatch], wait: float):
    while not STOP_FLAG.is_set():
        start_total_time = time.time()
        total_bytes = 0

        for i, batch in enumerate(batches, start=1):
            if STOP_FLAG.is_set():
                break

            batch_start = time.time()
            writer.write_batch(batch)
            batch_duration = time.time() - batch_start
            mb = batch.nbytes / (1000 ** 2)
            mbps = mb / batch_duration
            print(
                f"[Thread {thread_id}: {addr}] Sent Batch [{i}] ({mb:.2f} MB) in {batch_duration:.2f}s ({mbps:.2f} MB/s)")
            total_bytes += batch.nbytes
            time.sleep(wait)

        if STOP_FLAG.is_set():
            return

        total_duration = time.time() - start_total_time
        total_mb = total_bytes / (1000 ** 2)
        total_mbps = total_mb / total_duration
        print(f"[Thread {thread_id}: {addr}] Sent all {len(batches)} Batches ({total_mb:.2f} MB) in {total_duration:.2f}s ({total_mbps:.2f} MB/s)")

if __name__ == "__main__":
    args = parse_arguments()

    table: pa.Table = generate_table(
        num_rows=args.overall_tuples,
        event_type=args.stream.split(".")[1],
        generator_executable=args.generator_executable,
        offset=args.offset,
        step=args.step
    )

    batches: list[pa.RecordBatch] = table.to_batches(max_chunksize=args.tuples_per_batch)

    if args.store_input:
        store_input(table, args.store_input)

    processing_nodes = parse_nodes(args.processing_nodes)
    clients = connect_to_processing_nodes(processing_nodes, table.schema)

    sender_threads = start_multi_thread(
        clients=clients,
        batches=batches,
        wait=args.timer,
        thread_count=args.thread_count
    )

    try:
        while any(t.is_alive() for t in sender_threads):
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("Stopping source node...")
        STOP_FLAG.set()

    for t in sender_threads:
        t.join(5)

    print("Shutdown complete.")
