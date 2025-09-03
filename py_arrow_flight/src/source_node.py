import argparse
import json
import subprocess
import sys
import threading
import time

import pyarrow as pa
import pyarrow.flight


def generate_table(num_rows=10**6, event_type="person", generator_executable="nexmark"):
    cmd = [generator_executable, "-n", str(num_rows), "--type", event_type, "--no-wait"]
    print("Generate data..")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    records = []

    try:
        for line in proc.stdout:
            try:
                record = json.loads(line)
                records.append(record["Person"])
            except json.JSONDecodeError:
                continue
    finally:
        proc.stdout.close()
        proc.kill()
        proc.wait()

    print("Done.")
    return pa.Table.from_pylist(records)


def send_data(thread_id, schema, batches, processing_nodes):
    writers = []
    for processing_node in processing_nodes:
        client = pa.flight.FlightClient(
            f"grpc://{processing_node[0]}:{processing_node[1]}"
        )
        writer, _ = client.do_put(
            pa.flight.FlightDescriptor.for_path("bandwidth-test"), schema
        )
        writers.append(writer)

    for i, batch in enumerate(batches):
        writers[(thread_id + i) % len(processing_nodes)].write_batch(batch)

    for writer in writers:
        writer.done_writing()


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
    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Source Node Parameters")
    print("=" * 40)
    print(f" Stream Type                : {args.stream}")
    print(f" Overall Tuples             : {args.overall_tuples}")
    print(f" Tuples Per Batch           : {args.tuples_per_batch}")
    print(f" Processing Nodes           : {args.processing_nodes}")
    print(f" Thread Count               : {args.thread_count}")
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
    )
    batches = table.to_batches(max_chunksize=args.tuples_per_batch)

    threads = []
    for thread_id in range(args.thread_count):
        t = threading.Thread(
            target=send_data,
            args=(
                thread_id,
                table.schema,
                batches[thread_id :: args.thread_count],
                processing_nodes,
            ),
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
