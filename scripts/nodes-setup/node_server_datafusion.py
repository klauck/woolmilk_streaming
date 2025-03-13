#!/usr/bin/env python3
import sys
import json
import pyarrow as pa
import pyarrow.flight as fl
from datafusion import SessionContext

class NodeFlightServer(fl.FlightServerBase):
    def __init__(self, parquet_registrations, host="0.0.0.0", port=8815, chunk_size=100000, **kwargs):
        location = f"grpc://{host}:{port}"
        super().__init__(location, **kwargs)

        self._host = host
        self._port = port
        self._chunk_size = chunk_size

        self.ctx = SessionContext()
        for table_name, parquet_path in parquet_registrations.items():
            self.ctx.register_parquet(table_name, parquet_path)

        self.rows_sent = {}

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
                    auction IN (1007, 1020, 2001, 2019, 2087)
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

    def do_get(self, context, ticket):
        command = ticket.ticket.decode("utf-8")
        base_sql = self.get_base_sql(command)

        offset = self.rows_sent.get(command, 0)

        # Build a query for the next chunk of rows
        chunk_query = f"""
            SELECT *
            FROM ({base_sql}) AS sub
            LIMIT {self._chunk_size}
            OFFSET {offset}
        """

        chunk_table = self.ctx.sql(chunk_query).to_arrow_table()

        if chunk_table.num_rows == 0:
            empty_schema = chunk_table.schema
            empty_reader = pa.RecordBatchReader.from_batches(empty_schema, [])
            return fl.RecordBatchStream(empty_reader)
        else:
            self.rows_sent[command] = offset + chunk_table.num_rows
            return fl.RecordBatchStream(chunk_table.to_reader())

def run_server(parquet_files, host, port, chunk_size):
    server = NodeFlightServer(
        parquet_registrations=parquet_files,
        host=host,
        port=port,
        chunk_size=chunk_size
    )
    print(f"Serving Flight on {host}:{port} with chunk_size={chunk_size}")
    server.serve()

if __name__ == "__main__":
    
    if len(sys.argv) < 2:
        print("Usage: node_server_datafusion.py <node_config.json>")
        sys.exit(1)

    config_file = sys.argv[1]
    with open(config_file, "r") as f:
        node_config = json.load(f)

    print("Starting server...")

    # Pull out the relevant fields; fall back to defaults if missing
    chunk_size   = node_config.get("chunk_size", 100000)
    host         = node_config.get("serving_host", "0.0.0.0")
    serving_port = node_config.get("serving_port", 8815)
    parquet_files = node_config.get("parquet_files", {})

    print(f"Starting server with config: {node_config}")

    run_server(
        parquet_files=parquet_files,
        host=host,
        port=serving_port,
        chunk_size=chunk_size
    )
