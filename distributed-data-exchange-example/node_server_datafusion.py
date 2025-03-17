import pyarrow as pa
import pyarrow.flight as fl
from datafusion import SessionContext
import os

class NodeFlightServer(fl.FlightServerBase):
    def __init__(self, parquet_registrations, host="0.0.0.0", port=8815, chunk_size = 100000, **kwargs):
        location = f"grpc://{host}:{port}"
        super().__init__(location, **kwargs)

        self._host = host
        self._port = port
        
        self.CHUNK_SIZE = chunk_size

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
            total_records=-1,  # unknown
            total_bytes=-1     # unknown
        )

    def do_get(self, context, ticket):
        command = ticket.ticket.decode("utf-8")
        base_sql = self.get_base_sql(command)

        offset = self.rows_sent.get(command, 0)

        # Build a query for the next chunk of rows
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
            self.rows_sent[command] = offset + chunk_table.num_rows
            return fl.RecordBatchStream(chunk_table.to_reader())


if __name__ == "__main__":
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(current_file_dir, "..", "data")
    node_index = 1

    parquet_files = {
        "bids":     os.path.join(data_dir, f"bid_{node_index}.parquet"),
        "auctions": os.path.join(data_dir, f"auction_{node_index}.parquet"),
        "persons":  os.path.join(data_dir, f"person_{node_index}.parquet"),
    }

    server = NodeFlightServer(
        parquet_registrations=parquet_files,
        host="0.0.0.0",
        port=8815
    )

    print(f"Serving Flight on 0.0.0.0:{8815}")
    server.serve()
