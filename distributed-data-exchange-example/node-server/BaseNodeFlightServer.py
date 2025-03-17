import pyarrow.flight as fl
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