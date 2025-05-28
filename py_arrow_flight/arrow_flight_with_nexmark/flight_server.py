import pyarrow as pa
import time
import sys
import pyarrow.flight

class ProcessorNodeServer(pa.flight.FlightServerBase):
    def __init__(self, location, exit_location):
        super().__init__(location)
        self.exit_client = pa.flight.FlightClient(exit_location)

    def do_put(self, context, descriptor, reader, writer):
        exit_writer, _ = self.exit_client.do_put(
            descriptor,
            reader.schema
        )
        total_bytes = 0
        start = time.time()
        for chunk in reader:
            batch = chunk.data
            # execute and forward data here
            total_bytes += batch.nbytes
            exit_writer.write_batch(batch)
            print(f"Received {batch.num_rows} rows, {batch.nbytes} bytes")
        end = time.time()

        exit_writer.done_writing()

        duration = end - start
        mbps = (total_bytes * 8) / (duration * 1024 * 1024)
        print(f"Received {total_bytes} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps)")


if __name__ == "__main__":
    exit_server = "grpc://localhost:8915"

    if len(sys.argv) == 2:
        exit_server = f"grpc://{sys.argv[1]}"
    
    server = ProcessorNodeServer("grpc://0.0.0.0:8815", exit_server)
   
    print("Flight server running on port 8815")
    server.serve()
