import pyarrow as pa
import pyarrow.flight
import sys
import time

class BandwidthTestServer(pa.flight.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)

    def do_put(self, context, descriptor, reader, writer):
        total_bytes = 0
        start = time.time()
        for chunk in reader:
            batch = chunk.data
            # execute and forward data here
            total_bytes += batch.nbytes
        end = time.time()

        duration = end - start
        mbps = (total_bytes * 8) / (duration * 1024 * 1024)
        print(f"Received {total_bytes} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"USAGE: python {sys.argv[0]} PORT")
        exit(1)
    port = int(sys.argv[1])
    server = BandwidthTestServer(f"grpc://0.0.0.0:{port}")
    print(f"Flight server running on port {port}")
    server.serve()
