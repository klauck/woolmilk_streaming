from datafusion import column, literal, SessionContext
import pyarrow as pa
import pyarrow.flight
import sys
import time

class ProcessingNode(pa.flight.FlightServerBase):
    def __init__(self, location, forward_node):
        super().__init__(location)
        self.client = pa.flight.FlightClient(f"grpc://{forward_node}")
        # self.state = {}

    def do_put(self, context, descriptor, reader, writer):
        ctx = SessionContext()
        writer, _ = self.client.do_put(
            pa.flight.FlightDescriptor.for_path("bandwidth-test"),
            schema=pa.schema([('column', pa.float64())])
        )

        total_bytes = 0

        start = time.time()
        for chunk in reader:
            batch = chunk.data

            # Option 1: Dataframe API
            # df = ctx.create_dataframe([[batch]])
            # df.filter(column("column") < literal(0.5))


            # Option 2: SQL
            ctx.register_record_batches("values", [[batch]])
            df = ctx.sql("SELECT column FROM values WHERE column > 0.5")

            # df.show()

            for batch in df.collect():
                writer.write_batch(batch)
                total_bytes += batch.nbytes
            ctx.deregister_table("values")
        writer.done_writing()
        end = time.time()

        duration = end - start
        gbps = (total_bytes * 8) / (duration * 1000**3)
        mbps = total_bytes / (duration * 1000**2)

        print(f"Forwarded {total_bytes} bytes in {duration:.2f} seconds; {gbps:.4f} Gbps ({mbps:.2f} MBps)")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"USAGE: python {sys.argv[0]} PORT FORWARD_NODE")
        exit(1)
    port = sys.argv[1]
    forward_node = sys.argv[2]
    server = ProcessingNode(f"grpc://0.0.0.0:{port}", forward_node)
    print(f"Flight processing node running on port {port}")
    server.serve()
