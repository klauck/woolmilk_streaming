import pyarrow as pa
import pyarrow.flight
import time
import numpy as np
import sys

def generate_table(num_rows=10**6):
    array = pa.array(np.random.rand(num_rows), type=pa.float64())
    table = pa.table([array], names=["column"])
    return table

def send_data(server):
    client = pa.flight.FlightClient(f"grpc://{server}:8815")

    table = generate_table()
    writer, _ = client.do_put(
        pa.flight.FlightDescriptor.for_path("bandwidth-test"),
        table.schema
    )
    start = time.time()
    for batch in table.to_batches(max_chunksize=65536):
        writer.write_batch(batch)
    writer.done_writing()
    end = time.time()

    total_bytes = table.nbytes
    duration = end - start
    mbps = (total_bytes * 8) / (duration * 1024 * 1024)
    print(f"Sent {total_bytes} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps)")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"USAGE: python {sys.argv[0]} HOST")
        exit(1)
    server = sys.argv[1]
    send_data(server)