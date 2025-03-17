import json
import os

import pyarrow as pa
import pyarrow.flight as fl
from BaseNodeFlightServer import BaseNodeFlightServer

class GroupNodeFlightServer(BaseNodeFlightServer):
    def __init__(self, parquet_registrations, host="0.0.0.0", port=8815, chunk_size=100000, **kwargs):
        super().__init__(parquet_registrations, host, port, chunk_size, **kwargs)
        # { command: rows_sent, ... }
        self.group_rows_sent = {}

    def do_get(self, context, ticket):
        config_str = ticket.ticket.decode("utf-8")
        config = json.loads(config_str)
        command = config["command"]

        base_sql = self.get_base_sql(command)
        offset = self.group_rows_sent.get(command, 0)

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
            # Update the global offset.
            self.group_rows_sent[command] = offset + chunk_table.num_rows
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

    server = GroupNodeFlightServer(
        parquet_registrations=parquet_files,
        host="0.0.0.0",
        port=8815
    )

    print(f"Serving Flight on 0.0.0.0:{8815}")
    server.serve()