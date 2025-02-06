import pyarrow as pa
import pyarrow.flight as fl
import pandas as pd

class SimpleFlightServer(fl.FlightServerBase):
    def __init__(self, host="0.0.0.0", port=8815):
        super().__init__(location=(host, port)) #pass the host and port to the base

        self._host = host
        self._port = port

        self.table = pa.Table.from_pandas(
            pd.DataFrame({
                "name": ["Stefan", "Usama", "Ahmad"],
                "id": [1, 2, 3]
            })
        )

    def get_flight_info(self, context, descriptor):
        if descriptor.command == b"users":
            schema = self.table.schema
            endpoints = [
                fl.FlightEndpoint(
                    ticket=b"users",
                    locations=[fl.Location.for_grpc_tcp(self._host, self._port)]
                )
            ]
            total_records = self.table.num_rows

            total_bytes = self.table.nbytes

            return fl.FlightInfo(
                schema,
                descriptor,
                endpoints,
                total_records,
                total_bytes
            )
        else:
            raise NotImplementedError(f"Unknown descriptor: {descriptor.command}")

    def do_get(self, context, ticket: fl.Ticket):
        if ticket.ticket == b"users":
            return fl.RecordBatchStream(self.table)
        else:
            raise NotImplementedError(
                f"Unknown ticket: {ticket.ticket}"
            )


if __name__ == "__main__":
    server = SimpleFlightServer(host="0.0.0.0", port=8815)
    print(f"Flight server is about to serve on port: {server.port}")

    try:
        server.serve()
    except KeyboardInterrupt:
        print("Shutting down server...")
        server.shutdown()
