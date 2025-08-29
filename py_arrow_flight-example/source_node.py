import pyarrow as pa
import pyarrow.flight
import time
import numpy as np
import sys
import threading

def generate_table(num_rows=10**6):
    array = pa.array(np.random.rand(num_rows), type=pa.float64())
    table = pa.table([array], names=["column"])
    return table

def send_data(thread_id, server):
    client = pa.flight.FlightClient(f"grpc://{server}")

    table = generate_table()
    writer, _ = client.do_put(
        pa.flight.FlightDescriptor.for_path("bandwidth-test"),
        table.schema
    )
    start = time.time()
    send_times = []
    for batch in table.to_batches(max_chunksize=10000):
        send_start = time.time()
        writer.write_batch(batch)
        send_end = time.time()
        send_times.append((send_start, send_end))
    writer.done_writing()

    end = time.time()

    total_bytes = table.nbytes
    duration = end - start
    gbps = (total_bytes * 8) / (duration * 1000 ** 3)
    mbps = total_bytes / (duration * 1000 ** 2)

    print(f"Start: {start}")
    print(f"{thread_id}: Sent {total_bytes / 1000**2} MB in {duration:.7f} seconds; {gbps:.4f} Gbps ({mbps:.2f} MBps)")
    print('send_times = ', send_times)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"USAGE: python {sys.argv[0]} SERVER")
        exit(1)
    server = sys.argv[1]

    threads = []
    for thread_id in range(1):
        t = threading.Thread(target=send_data, args=(thread_id, server))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
