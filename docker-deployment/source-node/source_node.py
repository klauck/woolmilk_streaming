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

    print(f"[Thread {thread_id}] Start: {start}")
    print("send_times = ", send_times)
    print(
        f"{thread_id}: Sent {total_bytes / 1000 ** 2} MB in {duration:.7f} seconds; "
        f"{gbps:.4f} Gbps ({mbps:.2f} MBps)"
    )


if __name__ == "__main__":
    stream = os.getenv("STREAM", "nexmark.person")
    generator_executable = os.getenv("GENERATOR_EXECUTABLE", "nexmark")
    overall_tuples = int(os.getenv("OVERALL_TUPLES", "100000"))
    tuples_per_batch = int(os.getenv("TUPLES_PER_BATCH", "10000"))
    offset = int(os.getenv("OFFSET", "0"))
    step = int(os.getenv("STEP", "1"))
    processing_nodes_str = os.getenv("PROCESSING_NODES", "localhost:8010")
    thread_count = int(os.getenv("THREAD_COUNT", "1"))
    store_input = os.getenv("STORE_INPUT", "")

    print("\n" + "=" * 40)
    print(" WoolMilk Source Node Parameters")
    print("=" * 40)
    print(f" Stream Type                : {stream}")
    print(f" Overall Tuples             : {overall_tuples}")
    print(f" Tuples Per Batch           : {tuples_per_batch}")
    print(f" Offset                     : {offset}")
    print(f" Step                       : {step}")
    print(f" Processing Nodes           : {processing_nodes_str}")
    print(f" Thread Count               : {thread_count}")
    print(f" Store Input                : {store_input}")
    print("=" * 40 + "\n")

    processing_nodes = []
    for address in processing_nodes_str.split(","):
        host, port = address.split(":")
        processing_nodes.append((host, int(port)))

    if len(processing_nodes) == 0:
        print("No server addresses for processing nodes provided. Exiting.")
        sys.exit(1)

    event_type = stream.split(".")[1]

    table = generate_table(
        num_rows=overall_tuples,
        event_type=event_type,
        generator_executable=generator_executable,
        offset=offset,
        step=step,
    )
    batches = table.to_batches(max_chunksize=tuples_per_batch)

    if store_input != "":
        input_file = Path(store_input)
        input_folder = input_file.parent
        os.makedirs(input_folder, exist_ok=True)
        table = pa.Table.from_batches(batches)
        pq.write_table(table, f"{input_file}")
        print(f"Wrote .. {input_file}")

    threads = []
    for thread_id in range(thread_count):
        t = threading.Thread(
            target=send_data,
            args=(
                thread_id,
                table.schema,
                batches[thread_id :: thread_count],
                processing_nodes,
            ),
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()