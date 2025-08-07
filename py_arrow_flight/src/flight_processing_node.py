from datafusion import column, literal, SessionContext
import pyarrow as pa
import pyarrow.flight
import sys
import time
import argparse

class BandwidthTestServer(pa.flight.FlightServerBase):
    def __init__(self, location, exit_node, sql_query):
        super().__init__(location)
        self.client = pa.flight.FlightClient(f"grpc://{exit_node}")
        self.query = sql_query
        self.default_table_name = "nexmark_data"

    def do_put(self, context, descriptor, reader, writer):
        ctx = SessionContext()
        total_bytes = 0
        start = time.time()

        first_chunk = next(reader)
        batch = first_chunk.data

        # register table
        ctx.register_record_batches(self.default_table_name, [[batch]])
        if self.query:
            result_df = ctx.sql(self.query)
        else:
            result_df = df

        schema = result_df.schema()
        exit_writer, _ = self.client.do_put(
            pa.flight.FlightDescriptor.for_path(self.query or self.default_table_name),
            schema
        )

        for out_batch in result_df.collect():
            exit_writer.write_batch(out_batch)
            total_bytes += out_batch.nbytes

        # deregister table
        ctx.deregister_table(self.default_table_name)

        for chunk in reader:
            batch = chunk.data
            ctx.register_record_batches(self.default_table_name, [[batch]])
            df = ctx.table(self.default_table_name)
            if self.query:
                result_df = ctx.sql(self.query)
            else:
                result_df = df
            for out_batch in result_df.collect():
                exit_writer.write_batch(out_batch)
                total_bytes += out_batch.nbytes
            ctx.deregister_table(self.default_table_name)

        exit_writer.done_writing()
        end = time.time()

        duration = end - start
        mbps = (total_bytes * 8) / (duration * 1024 * 1024)
        print(f"Received {total_bytes} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Arrow Flight Processing Node")
    parser.add_argument(
        "--server-address",
        type=str,
        default="localhost:8815",
        help="Address to run the Flight processing node on (host:port)"
    )
    parser.add_argument(
        "--exit_node",
        type=str,
        default="localhost:8820",
        help="Address of the exit Flight node (host:port)"
    )
    parser.add_argument(
        "--query",
        type=str,
        default="SELECT * FROM nexmark_data",
        help="Optional SQL query to run on incoming batches"
    )
    args = parser.parse_args()

    print("\n" + "="*40)
    print(" Arrow Flight Processing Node Parameters")
    print("="*40)       
    print(f" Address        : {args.server_address}")
    print(f" Exit Node      : {args.exit_node}")
    print(f" SQL Query      : {args.query}")
    print("="*40 + "\n")

    address = args.server_address
    exit_node = args.exit_node
    sql_query = args.query

    server = BandwidthTestServer(f"grpc://{address}", exit_node, sql_query)
    print(f"Flight processing node running at {address}")
    server.serve()
