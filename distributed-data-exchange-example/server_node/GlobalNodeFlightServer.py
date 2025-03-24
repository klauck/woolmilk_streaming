import json
import threading

import pyarrow as pa
import pyarrow.flight as fl
from .BaseNodeFlightServer import BaseNodeFlightServer

class GlobalNodeFlightServer(BaseNodeFlightServer):
    """
    A Flight server that serves data to ProcessorNodes, with global state.
    """
    def __init__(self, parquet_registrations, host="0.0.0.0", port=8815, chunk_size=100000, **kwargs):
        """
        Args:
            parquet_registrations: Dict of table name to parquet file path.
            host: Host for the server.
            port: Port for the server.
            chunk_size: Number of rows to return per chunk.
        """
        super().__init__(parquet_registrations, host, port, chunk_size, **kwargs)
        # { command: rows_sent, ... }
        self.group_rows_sent = {}
        # Lock to serialize SQL queries, since self.ctx is not thread-safe.
        self.query_lock = threading.Lock()

    def do_get(self, context, ticket):
        # config_str is a JSON string with the command and node_id.
        config_str = ticket.ticket.decode("utf-8")
        config = json.loads(config_str)
        command = config["command"]

        # Enter the critical section to ensure that the query is executed atomically and the offset is updated correctly.
        with self.query_lock:
            base_sql = self.get_base_sql(command)
            offset = self.group_rows_sent.get(command, 0)

            chunk_query = f"""
                SELECT *
                FROM ({base_sql}) AS sub
                LIMIT {self.CHUNK_SIZE}
                OFFSET {offset}
            """
            
            chunk_table = self.ctx.sql(chunk_query).to_arrow_table()

            if chunk_table.num_rows == 0:
                empty_schema = self.ctx.sql(f"{base_sql} LIMIT 0").to_arrow_table().schema
                empty_reader = pa.RecordBatchReader.from_batches(empty_schema, [])
                return fl.RecordBatchStream(empty_reader)
            else:
                # Update the global offset.
                self.group_rows_sent[command] = offset + chunk_table.num_rows
                return fl.RecordBatchStream(chunk_table.to_reader())

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start GlobalNodeFlightServer")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Server host")
    parser.add_argument("--port", type=int, default=8815, help="Server port")
    parser.add_argument("--registrations", type=str, required=True, 
                        help="Parquet registrations as JSON string. For example: '{\"bids\": \"/path/to/bid_1.parquet\", \"auctions\": \"/path/to/auction_1.parquet\", \"persons\": \"/path/to/person_1.parquet\"}'")
    args = parser.parse_args()

    try:
        parquet_registrations = json.loads(args.registrations)
    except Exception as e:
        print("Error parsing registrations JSON:", e)
        exit(1)

    server = GlobalNodeFlightServer(
        parquet_registrations=parquet_registrations,
        host=args.host,
        port=args.port
    )

    print(f"Serving Flight on {args.host}:{args.port}")
    server.serve()