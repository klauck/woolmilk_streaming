from BaseNodeFlightServer import BaseNodeFlightServer
import json
import pyarrow as pa
import pyarrow.flight as fl
import os

class IndividualNodeFlightServer(BaseNodeFlightServer):
    def __init__(self, parquet_registrations, host="0.0.0.0", port=8815, chunk_size=100000, **kwargs):
        super().__init__(parquet_registrations, host, port, chunk_size, **kwargs)
        # { node_id: { command: rows_sent, ... }, ... }
        self.node_info = {}

    def do_get(self, context, ticket):
        config_str = ticket.ticket.decode("utf-8")
        config = json.loads(config_str)
        command = config["command"]
        node_id = config["node_id"]

        # Ensure we have an entry for this node.
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

if __name__ == "__main__":
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(current_file_dir, "../..", "data")
    node_index = 1

    parquet_files = {
        "bids":     os.path.join(data_dir, f"bid_{node_index}.parquet"),
        "auctions": os.path.join(data_dir, f"auction_{node_index}.parquet"),
        "persons":  os.path.join(data_dir, f"person_{node_index}.parquet"),
    }

    server = IndividualNodeFlightServer(
        parquet_registrations=parquet_files,
        host="0.0.0.0",
        port=8815
    )

    print(f"Serving Flight on 0.0.0.0:{8815}")
    server.serve()
