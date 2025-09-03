import os
import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.parquet as pq

CHUNK_SIZE_IN_ROWS = 200000

base_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(base_dir, "..", "data")

class NodeFlightServerOnDemand(fl.FlightServerBase):
    def __init__(self, data_dir, host="0.0.0.0", port=8815, **kwargs):
        self._host = host
        self._port = port
        location = f"grpc://{host}:{port}"
        super().__init__(location, **kwargs)

        self.auctions_file = pq.ParquetFile(os.path.join(data_dir, "auctions.parquet"))
        self.bids_file = pq.ParquetFile(os.path.join(data_dir, "bids.parquet"))
        self.persons_file = pq.ParquetFile(os.path.join(data_dir, "persons.parquet"))
        
        self.iterators = {
            "auctions": self.auctions_file.iter_batches(batch_size=CHUNK_SIZE_IN_ROWS),
            "bids": self.bids_file.iter_batches(batch_size=CHUNK_SIZE_IN_ROWS),
            "persons": self.persons_file.iter_batches(batch_size=CHUNK_SIZE_IN_ROWS)
        }

        self.schemas = {
            "auctions": self.auctions_file.metadata.schema.to_arrow_schema(),
            "bids": self.bids_file.metadata.schema.to_arrow_schema(),
            "persons": self.persons_file.metadata.schema.to_arrow_schema(),
        }

    def construct_flight_info(self, parquet_file, name, descriptor):
        descriptor = fl.FlightDescriptor.for_command(name)
        total_records = parquet_file.metadata.num_rows

        total_bytes = 0

        schema = parquet_file.metadata.schema.to_arrow_schema()

        endpoints = [
            fl.FlightEndpoint(
                ticket=bytes(name, "utf-8"),
                locations=[fl.Location.for_grpc_tcp(self._host, self._port)]
            )
        ]

        return fl.FlightInfo(
            schema=schema,
            descriptor=descriptor,
            endpoints=endpoints,
            total_records=total_records,
            total_bytes=total_bytes
        )

    def list_flights(self, context, descriptor):
        auctions_info = self.construct_flight_info(self.auctions_file, "auctions", descriptor)
        bids_info = self.construct_flight_info(self.bids_file, "bids", descriptor)
        persons_info = self.construct_flight_info(self.persons_file, "persons", descriptor)

        return [auctions_info, bids_info, persons_info]

    def get_flight_info(self, context, descriptor):
        cmd = descriptor.command
        if cmd == b"auctions":
            return self.construct_flight_info(self.auctions_file, "auctions", descriptor)
        elif cmd == b"bids":
            return self.construct_flight_info(self.bids_file, "bids", descriptor)
        elif cmd == b"persons":
            return self.construct_flight_info(self.persons_file, "persons", descriptor)
        else:
            raise fl.FlightInternalError(f"Unknown descriptor command: {cmd}")

    def do_get(self, context, ticket):
        name = ticket.ticket.decode("utf-8")

        if name not in self.iterators:
            raise fl.FlightInternalError(f"Unknown ticket: {name}")

        try:
            batch = next(self.iterators[name])
            chunk_table = pa.Table.from_batches([batch])
        except StopIteration:
            empty_reader = pa.RecordBatchReader.from_batches(self.schemas[name], [])
            return fl.RecordBatchStream(empty_reader)

        batch_reader = chunk_table.to_reader()
        return fl.RecordBatchStream(batch_reader)

if __name__ == "__main__":
    server = NodeFlightServerOnDemand(data_dir=data_dir, host="0.0.0.0", port=8815)
    print(f"On-Demand Flight server running on port: {server._port}")
    server.serve()
