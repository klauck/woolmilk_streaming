import argparse
import time

import pyarrow as pa
import pyarrow.flight
from datafusion import SessionContext


class ProcessingNode(pa.flight.FlightServerBase):
    def __init__(self, location, exit_node, sql_query):
        super().__init__(location)
        self.forwarding_client = pa.flight.FlightClient(f"grpc://{exit_node}")
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
        forward_writer, _ = self.forwarding_client.do_put(
            pa.flight.FlightDescriptor.for_path(self.query or self.default_table_name),
            schema,
        )

        for out_batch in result_df.collect():
            forward_writer.write_batch(out_batch)
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
                forward_writer.write_batch(out_batch)
                total_bytes += out_batch.nbytes
            ctx.deregister_table(self.default_table_name)

        forward_writer.done_writing()
        end = time.time()

        duration = end - start
        mbps = (total_bytes * 8) / (duration * 1024 * 1024)
        print(
            f'WM_LOG= {{"received_bytes": {total_bytes}, "start_time": {start},'
            f' "duration": {duration}, "Mbps": {mbps:.2f}}}'
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Processing Node")
    parser.add_argument(
        "--server-address",
        type=str,
        default="localhost:8815",
        help="Address to run the WoolMilk processing node (host:port)",
    )
    parser.add_argument(
        "--forward_node",
        type=str,
        default="localhost:8820",
        help="Address of the node to forward data to (host:port)",
    )
    parser.add_argument(
        "--query",
        type=str,
        default="SELECT * FROM nexmark_data",
        help="SQL query to run on incoming batches",
    )
    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Processing Node Parameters")
    print("=" * 40)
    print(f" Address        : {args.server_address}")
    print(f" Forward Node   : {args.forward_node}")
    print(f" SQL Query      : {args.query}")
    print("=" * 40 + "\n")

    address = args.server_address
    forward_node = args.forward_node
    sql_query = args.query

    processing_node = ProcessingNode(f"grpc://{address}", forward_node, sql_query)
    print(f"WoolMilk processing node running at {address}")
    processing_node.serve()
