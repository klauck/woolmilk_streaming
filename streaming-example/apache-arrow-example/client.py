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
