import pyarrow as pa
import time
import sys
from nexmark_data_generator import NexmarkDataGenerator

MAX_CHUNK_SIZE = 65536
NO_RECORDS = 1000000
RECORDS_PER_CHUNK = 100000

def send_data(server):
    client = pa.flight.FlightClient(f"grpc://{server}:8815")
    data_generator = NexmarkDataGenerator(chunk_size=RECORDS_PER_CHUNK, no_records=NO_RECORDS)

    category_schema, person_schema, auction_schema, bid_schema = data_generator.get_schemas()

    writer, _ = client.do_put(
        pa.flight.FlightDescriptor.for_path("nexmark_data"),
        bid_schema
    )

    time_history = []
    total_bytes = 0

    for person_tbl, auction_tbl, bid_tbl, category_tbl in data_generator.generate():
        time_history.append(time.time())
        total_bytes += bid_tbl.nbytes
        for batch in bid_tbl.to_batches(max_chunksize=MAX_CHUNK_SIZE):
            writer.write_batch(batch)
        time_history.append(time.time())
        print(f"Sent {bid_tbl.num_rows} rows, {bid_tbl.nbytes} bytes in {time_history[-1] - time_history[-2]:.2f} seconds")

    writer.done_writing()

    duration = 0
    for i in range(1, len(time_history)):
        duration += time_history[i] - time_history[i - 1]
    mbps = (total_bytes * 8) / (duration * 1024 * 1024)
    print(f"Sent {total_bytes} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps)")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"USAGE: python {sys.argv[0]} HOST")
        exit(1)
    server = sys.argv[1]
    send_data(server)