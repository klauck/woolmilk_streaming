from datafusion import column, literal, SessionContext
import pyarrow as pa
import pyarrow.flight
import sys
import time

class BandwidthTestServer(pa.flight.FlightServerBase):
    def __init__(self, location, exit_node):
        super().__init__(location)
        self.ctx = SessionContext()
        self.client = pa.flight.FlightClient(f"grpc://{exit_node}")
        # self.state = {}

    def do_put(self, context, descriptor, reader, writer):
        writer, _ = self.client.do_put(
            pa.flight.FlightDescriptor.for_path("bandwidth-test"),
            schema=pa.schema([('column', pa.float64())])
        )

        total_bytes = 0
        start = time.time()
        for chunk in reader:
            batch = chunk.data

            # Option 1: Dataframe API
            # df = self.ctx.create_dataframe([[batch]])
            # df.filter(column("column") < literal(0.5))

            # Option 2: SQL
            self.ctx.deregister_table("values")
            self.ctx.register_record_batches("values", [[batch]])
            df = self.ctx.sql("SELECT max(column) as column FROM values WHERE column < 0.5")

            df.show()

            for batch in df.collect():
                writer.write_batch(batch)
                total_bytes += batch.nbytes
        writer.done_writing()
        end = time.time()

        duration = end - start
        mbps = (total_bytes * 8) / (duration * 1024 * 1024)
        print(f"Received {total_bytes} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps)")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"USAGE: python {sys.argv[0]} PORT EXIT_NODE")
        exit(1)
    port = sys.argv[1]
    exit_node = sys.argv[2]
    server = BandwidthTestServer(f"grpc://0.0.0.0:{port}", exit_node)
    print(f"Flight processing node running on port {port}")
    server.serve()
