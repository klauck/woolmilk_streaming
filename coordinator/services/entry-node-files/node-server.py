import json
import os

import pyarrow as pa
import pyarrow.flight as fl
import argparse
from datafusion import SessionContext

class BaseNodeFlightServer(fl.FlightServerBase):
    def __init__(self, parquet_registrations, host="0.0.0.0", port=8815, chunk_size=100000, **kwargs):
        location = f"grpc://{host}:{port}"
        super().__init__(location, **kwargs)

        self._host = host
        self._port = port
        self.CHUNK_SIZE = chunk_size

        self.ctx = SessionContext()
        for table_name, parquet_path in parquet_registrations.items():
            self.ctx.register_parquet(table_name, parquet_path)

    def get_base_sql(self, command):
        if command == "nexmarkq1":
            return "SELECT auction, price, bidder, date_time FROM bids"
        elif command == "nexmarkq2":
            return """
                SELECT
                    auction,
                    price
                FROM bids
                WHERE
                    auction = 1007
                    OR auction = 1020
                    OR auction = 2001
                    OR auction = 2019
                    OR auction = 2087
            """
        else:
            raise fl.FlightInternalError(f"Unknown command '{command}'")

    def get_flight_info(self, context, descriptor):
        command = descriptor.command.decode("utf-8")
        base_sql = self.get_base_sql(command)
        schema_query = f"SELECT * FROM ({base_sql}) AS sub LIMIT 0"

        schema_table = self.ctx.sql(schema_query).to_arrow_table()
        schema = schema_table.schema

        endpoints = [
            fl.FlightEndpoint(
                ticket=fl.Ticket(descriptor.command),
                locations=[fl.Location.for_grpc_tcp(self._host, self._port)]
            )
        ]

        return fl.FlightInfo(
            schema=schema,
            descriptor=descriptor,
            endpoints=endpoints,
            total_records=-1,
            total_bytes=-1
        )

class GroupNodeFlightServer(BaseNodeFlightServer):
    def __init__(self, parquet_registrations, host="0.0.0.0", port=8815, chunk_size=100000, **kwargs):
        super().__init__(parquet_registrations, host, port, chunk_size, **kwargs)
        # { command: rows_sent, ... }
        self.group_rows_sent = {}

    def do_get(self, context, ticket):
        config_str = ticket.ticket.decode("utf-8")
        config = json.loads(config_str)
        command = config["command"]

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

def main():
    parser = argparse.ArgumentParser(description="Group Node Flight Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8815, help="Port to bind to (default: 8815)")
    parser.add_argument(
        "--data_dir",
        default=os.path.join("..", "..", "data"),
        help="Base directory containing the Parquet files (default: ../../data)"
    )
    parser.add_argument(
        "--parquet_files",
        required=True,
        help=(
            "JSON string specifying relative Parquet file names for each dataset. "
            "Example: '{\"bids\": \"bid.parquet\", \"auctions\": \"auction.parquet\", \"persons\": \"person.parquet\"}'"
        )
    )

    args = parser.parse_args()

    # Load and parse the parquet file dictionary from the JSON string
    parquet_files = json.loads(args.parquet_files)

    # Prepend each file with the specified data directory (ignoring any node_index)
    for key, file_name in parquet_files.items():
        parquet_files[key] = os.path.join(args.data_dir, file_name)

    # Create and start the server
    server = GroupNodeFlightServer(
        parquet_registrations=parquet_files,
        host=args.host,
        port=args.port
    )

    print(f"Serving Flight on {args.host}:{args.port}")
    server.serve()


if __name__ == "__main__":
    main()