import pyarrow as pa
import pyarrow.flight
import time

class ExitNodeServer(pa.flight.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)

    def do_put(self, context, descriptor, reader, writer):
        total_bytes = 0
        start = time.time()
        for chunk in reader:
            batch = chunk.data
            total_bytes += batch.nbytes
            print(f"Received {batch.num_rows} rows, {batch.nbytes} bytes")
        end = time.time()

        duration = end - start
        mbps = (total_bytes * 8) / (duration * 1024 * 1024)
        print(f"Received {total_bytes} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps)")


if __name__ == "__main__":
    server = ExitNodeServer("grpc://0.0.0.0:8915")
    print("Flight server running on port 8915")
    server.serve()
