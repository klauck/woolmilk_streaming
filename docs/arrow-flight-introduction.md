# Apache Arrow Flight Introduction

Apache Arrow Flight is a Arrow RPC service that allows for high-performance data transport between systems. It is designed to efficiently transfer large datasets over a network. Arrow Flight is built on top of Apache Arrow and gRPC, Apache has slightly changed the gRPC protocol to make it more efficient for transferring Arrow data.

## Arrow Flight Architecture

Arrow Flight is a client-server system that allows clients to send Arrow data to a server and receive Arrow data from a server.

There are two main components in Arrow Flight:

1. **Flight Server**: The Flight server is a service that listens for incoming connections from clients.
2. **Flight Client**: The Flight client is a service that connects to a Flight server and sends or receives Arrow data.

There are few methods that are used to interact with the Flight server:

### Flight Info

Flight Info is a metadata object that describes a Flight service. It contains information about the service, such as the service name, the service description, and the service endpoints.

### do_get

The `do_get` method is used to get the data from the server.

### Do Put

The `do_put` method is used to put the on the server.

## Python Example

### server.py

```python
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
```

### client.py

```python
import pyarrow as pa
import pyarrow.flight as fl

if __name__ == "__main__":
    client = fl.FlightClient("grpc://localhost:8815")

    descriptor = fl.FlightDescriptor.for_command("users")
    flight_info = client.get_flight_info(descriptor)

    ticket = flight_info.endpoints[0].ticket

    reader = client.do_get(ticket)
    table = reader.read_all()

    print("Received table from server:")
    print(table.to_pandas())
```
