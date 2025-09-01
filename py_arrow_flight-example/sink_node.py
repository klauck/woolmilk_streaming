import sys
import time

import pyarrow as pa
import pyarrow.flight


class SinkNode(pa.flight.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)

    def do_put(self, context, descriptor, reader, writer):
        total_bytes = 0
        receive_times = []
        start = receive_start = time.time()
        for chunk in reader:
            batch = chunk.data
            # execute and forward data here
            total_bytes += batch.nbytes
            receive_end = time.time()
            receive_times.append((receive_start, receive_end))
            receive_start = receive_end
        end = time.time()

        duration = end - start
        gbps = (total_bytes * 8) / (duration * 1000**3)
        mbps = total_bytes / (duration * 1000**2)

        print(
            f"Received {total_bytes / 1000**2} MB in {duration:.7f} seconds; "
            f"{gbps:.4f} Gbps ({mbps:.2f} MBps)"
        )
        print(f"End: {end}")
        print("receive_times = ", receive_times)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"USAGE: python {sys.argv[0]} PORT")
        exit(1)
    port = int(sys.argv[1])
    server = SinkNode(f"grpc://0.0.0.0:{port}")
    print(f"Flight server running on port {port}")
    server.serve()
