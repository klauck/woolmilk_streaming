import json
import os

import pyarrow as pa
import pyarrow.flight as fl
import argparse
from datafusion import SessionContext
import pyarrow.parquet as pq

class BaseNodeFlightServer(fl.FlightServerBase):
    def __init__(self, parquet_registrations, host="0.0.0.0", port=8815, read_type="disk", chunk_size=1000000, **kwargs):
        location = f"grpc://{host}:{port}"
        super().__init__(location, **kwargs)

        self._host = host
        self._port = port
        self.CHUNK_SIZE = chunk_size

        self.ctx = SessionContext()
        self.read_type = read_type

        if read_type not in ["memory", "disk"]:
            raise ValueError("read_type must be either 'memory' or 'disk'")

        if read_type == "disk":
            for table_name, parquet_path in parquet_registrations.items():
                self.ctx.register_parquet(table_name, parquet_path)

        elif read_type == "memory":
            for table_name, parquet_path in parquet_registrations.items():
                arrow_table = pq.read_table(parquet_path, read_dictionary=False).replace_schema_metadata(None)
                total_size = arrow_table.nbytes
                self.ctx.from_arrow(arrow_table, name=table_name)

    def get_flight_info(self, context, descriptor):
        # Parse the JSON config passed in the descriptor.
        config_str = descriptor.command.decode("utf-8")
        config = json.loads(config_str)
        if "command_str" not in config:
            raise fl.FlightInternalError("Missing 'command_str' in ticket config")

        command_str = config["command_str"]
        # Wrap the provided SQL in a subquery to extract the schema.
        schema_query = f"SELECT * FROM ({command_str}) AS sub LIMIT 0"
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
    def __init__(self, parquet_registrations, host="0.0.0.0", port=8815, read_type = "disk", chunk_size=100000, **kwargs):
        super().__init__(parquet_registrations, host, port, read_type, chunk_size, **kwargs)
        # Keep track of rows sent for each command_str.
        self.group_rows_sent = {}  # { command_str: rows_sent }

    def do_get(self, context, ticket):
        config_str = ticket.ticket.decode("utf-8")
        config = json.loads(config_str)
        if "command_str" not in config:
            raise fl.FlightInternalError("Missing 'command_str' in ticket config")
        command_str = config["command_str"]

        offset = self.group_rows_sent.get(command_str, 0)

        chunk_query = f"""
            SELECT *
            FROM ({command_str}) AS sub
            LIMIT {self.CHUNK_SIZE}
            OFFSET {offset}
        """

        chunk_table = self.ctx.sql(chunk_query).to_arrow_table()

        if chunk_table.num_rows == 0:
            empty_schema_table = self.ctx.sql(f"{command_str} LIMIT 0").to_arrow_table()
            empty_schema = empty_schema_table.schema
            empty_reader = pa.RecordBatchReader.from_batches(empty_schema, [])
            return fl.RecordBatchStream(empty_reader)
        else:
            # Update offset.
            self.group_rows_sent[command_str] = offset + chunk_table.num_rows
            return fl.RecordBatchStream(chunk_table.to_reader())

def parse_parquet_files(files_str):
    """
    Expects a string of comma-separated key=value pairs, e.g.:
        "bids=bid_1.parquet,auctions=auction.parquet,persons=person.parquet"
    Returns a dict like:
        {"bids": "bid_1.parquet", "auctions": "auction.parquet", "persons": "person.parquet"}
    """
    parquet_dict = {}
    for pair in files_str.split(","):
        key, val = pair.split("=")
        parquet_dict[key.strip()] = val.strip()
    return parquet_dict


class IndividualNodeFlightServer(BaseNodeFlightServer):
    def __init__(self, parquet_registrations, host="0.0.0.0", port=8815, read_type = "disk", chunk_size=100000, **kwargs):
        super().__init__(parquet_registrations, host, port, read_type, chunk_size, **kwargs)
        self.node_info = {}

    def do_get(self, context, ticket):
        config_str = ticket.ticket.decode("utf-8")
        config = json.loads(config_str)
        command = config["command"]
        node_id = config["node_id"]

        if node_id not in self.node_info:
            self.node_info[node_id] = {}

        base_sql = self.get_base_sql(command)
        offset = self.node_info[node_id].get(command, 0)

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
            # Update the node-specific offset.
            self.node_info[node_id][command] = offset + chunk_table.num_rows
            return fl.RecordBatchStream(chunk_table.to_reader())


def main():
    parser = argparse.ArgumentParser(description="Group Node Flight Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8815, help="Port to bind to (default: 8815)")
    # file read type in memory or disk
    parser.add_argument(
        "--read_type",
        default="memory",
        choices=["memory", "disk"],
        help="Type of read for parquet files (default: memory)"
    )
    
    # const current file dir
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    parser.add_argument(
        "--data_dir",
        default=".",
        help="Base directory containing the Parquet files (default: ../../data)"
    )
    parser.add_argument(
        "--parquet_files",
        default="bid=bids.parquet",
        help=(
            "Comma-separated key=value pairs mapping dataset names to parquet filenames. "
            "Example: 'bids=bid_1.parquet,auctions=auction.parquet,persons=person.parquet'"
        )
    )

    parser.add_argument(
        "--bit-rate",
        type=int,
        default=1000000,
        help="Bitrate for the stream (default: 1000000)"
    )

    parser.add_argument(
        "--server-type",
        default="individual",
        choices=["individual", "group"],
        help="Type of server to run (default: individual)"
    )

    # python3 node-server.py --host 0.0.0.0 --port 8815 --read_type memory --data_dir /Users/usamabintariq/Documents/GitHub/woolmilk_streaming/data --parquet_files bids=bids.parquet,auctions=auctions.parquet,persons=persons.parquet

    args = parser.parse_args()

    data_dir = args.data_dir

    if data_dir == "." or data_dir == "./":
        data_dir = current_file_dir 

    # Build the dictionary from the key=value string
    parquet_files = parse_parquet_files(args.parquet_files)
    if data_dir is not None:
        for key, file_name in parquet_files.items():
            print(f"Loading {file_name} for {key}")
            parquet_files[key] = os.path.join(data_dir, file_name)

    print(parquet_files)

    server: BaseNodeFlightServer = None

    if args.server_type == "group":
        GroupNodeFlightServer(
            parquet_registrations=parquet_files,
            host=args.host,
            port=args.port,
            read_type=args.read_type,
            chunk_size=args.bit_rate,
        )
    else: 
        server = IndividualNodeFlightServer(
            parquet_registrations=parquet_files,
            host=args.host,
            port=args.port,
            read_type=args.read_type,
            chunk_size=args.bit_rate,
        )

    print(f"Serving Flight on {args.host}:{args.port}")
    server.serve()

if __name__ == "__main__":
    main()
