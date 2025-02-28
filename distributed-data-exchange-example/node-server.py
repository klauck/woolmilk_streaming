import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.parquet as pq
import os

CHUNK_SIZE_IN_ROWS = 100000

base_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(base_dir, "..", "data")

class NodeFlightServer(fl.FlightServerBase):
    def __init__(self, data_dir, host="0.0.0.0", port=8815, **kwargs):
        self._host = host
        self._port = port

        location = f"grpc://{host}:{port}"

        # for now, we are loading the entire parquet file into memory
        self.auctions = pq.read_table(os.path.join(data_dir, "auctions.parquet"))
        self.bids = pq.read_table(os.path.join(data_dir, "bids.parquet"))
        self.persons = pq.read_table(os.path.join(data_dir, "persons.parquet"))
        
        self.rows_sent = {
            "auctions": 0,
            "bids": 0,
            "persons": 0
        }

        super().__init__(location, **kwargs)


    def construct_flight_info(self, table, name, descriptor):
        descriptor = fl.FlightDescriptor.for_command(name)
        schema = table.schema
        endpoints = [fl.FlightEndpoint(ticket=bytes(name, "utf-8"), locations=[fl.Location.for_grpc_tcp(self._host, self._port)])]
        total_records = table.num_rows
        total_bytes = table.nbytes

        return fl.FlightInfo(schema, descriptor, endpoints, total_records, total_bytes)

    def list_flights(self, context, descriptor):
        auctions_info = self.construct_flight_info(self.auctions, "auctions", descriptor)
        bids_info = self.construct_flight_info(self.bids, "bids", descriptor)
        persons_info = self.construct_flight_info(self.persons, "persons", descriptor)

        return [auctions_info, bids_info, persons_info]

    def get_flight_info(self, context, descriptor):
        if descriptor.command == b"auctions":
            return self.construct_flight_info(self.auctions, "auctions", descriptor)
        
        if descriptor.command == b"bids":
            return self.construct_flight_info(self.bids, "bids", descriptor)
        
        if descriptor.command == b"persons":
            return self.construct_flight_info(self.persons, "persons", descriptor)

        raise fl.FlightInternalError("Unknown descriptor")
    
    def get_next_batch(self, table, name):
        rows_sent = self.rows_sent[name]

        if rows_sent >= table.num_rows:
            return None

        end = min(rows_sent + CHUNK_SIZE_IN_ROWS, table.num_rows)  # Ensure end doesn't exceed total rows

        slice_table = table.slice(rows_sent, CHUNK_SIZE_IN_ROWS)  # Fixed slicing
        self.rows_sent[name] = end

        return slice_table

    def do_get(self, context, ticket):
        table = None
        name = None

        if ticket.ticket == b"auctions":
            table = self.auctions
            name = "auctions"
        elif ticket.ticket == b"bids":
            table = self.bids
            name = "bids"
        elif ticket.ticket == b"persons":
            table = self.persons
            name = "persons"

        batch_data = self.get_next_batch(table, name)

        if batch_data is None:
            empty_reader = pa.RecordBatchReader.from_batches(table.schema, [])
            return fl.RecordBatchStream(empty_reader)
        else:
            batch_reader = batch_data.to_reader()
            return fl.RecordBatchStream(batch_reader)

if __name__ == "__main__":
    server = NodeFlightServer(data_dir)

    print(f"Flight server is about to serve on port: {server.port}")

    server.serve()
