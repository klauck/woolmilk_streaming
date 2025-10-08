import json
import os
import time

import pyarrow as pa
import pyarrow.flight
from datafusion import SessionContext


class ProcessingNode(pa.flight.FlightServerBase):
    def __init__(self, location, forward_node, sql_query, schema_json):
        super().__init__(location)
        self.forwarding_client = pa.flight.FlightClient(f"grpc://{forward_node}")
        self.query = sql_query
        self.default_table_name = "nexmark_data"

        if not schema_json:
            raise ValueError("Schema is mandatory. Please provide a valid schema.")

        self.predefined_schema = self._parse_schema(schema_json)
        print(self.predefined_schema)

    def _parse_schema(self, schema_json):
        schema_dict = json.loads(schema_json)
        fields = []
        for field in schema_dict.get("fields", []):
            field_name = field["name"]
            field_type = field["type"]

            if field_type == "int64":
                pa_type = pa.int64()
            elif field_type == "string":
                pa_type = pa.string()
            elif field_type == "float64":
                pa_type = pa.float64()
            else:
                pa_type = pa.string()

            fields.append(pa.field(field_name, pa_type))

        return pa.schema(fields)

    def do_put(self, context, descriptor, reader, writer):
        ctx = SessionContext()

        forward_writer, _ = self.forwarding_client.do_put(
            pa.flight.FlightDescriptor.for_path(self.query or self.default_table_name),
            schema=self.predefined_schema,
        )

        total_bytes = 0
        forwarding_times = []
        cost_break_down = {"receiving": [], "querying": [], "sending": []}
        forward_start = start = time.time()

        for chunk in reader:
            batch = chunk.data

            processing_start = time.time()
            ctx.register_record_batches(self.default_table_name, [[batch]])

            result_df = ctx.sql(self.query)

            result = result_df.collect()
            processing_end = time.time()

            for result_batch in result:
                forward_writer.write_batch(result_batch)
                total_bytes += result_batch.nbytes

            ctx.deregister_table(self.default_table_name)

            forward_end = time.time()
            forwarding_times.append((forward_start, forward_end))

            cost_break_down["receiving"].append(processing_start - forward_start)
            cost_break_down["querying"].append(processing_end - processing_start)
            cost_break_down["sending"].append(forward_end - processing_end)

            forward_start = forward_end

        forward_writer.done_writing()
        end = time.time()

        duration = end - start
        gbps = (total_bytes * 8) / (duration * 1000**3)
        mbps = total_bytes / (duration * 1000**2)

        print(
            f'WM_LOG= {{"received_bytes": {total_bytes}, "start_time": {start},'
            f' "duration": {duration}, "MBps": {mbps:.2f}, "Gbps": {gbps:.4f}}}'
        )
        print("  receiving: ", sum(cost_break_down["receiving"]))
        print("  querying: ", sum(cost_break_down["querying"]))
        print("  sending: ", sum(cost_break_down["sending"]))
        print("forward_times = ", forwarding_times)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8010"))
    forward_node = os.getenv("FORWARD_NODE", "localhost:8020")
    sql_query = os.getenv("QUERY", "SELECT * FROM nexmark_data")
    
    #TODO: get schema from env or config file

    default_schema = {
        "fields": [
            {"name": "id", "type": "int64"},
            {"name": "name", "type": "string"},
            {"name": "email_address", "type": "string"},
            {"name": "credit_card", "type": "string"},
            {"name": "city", "type": "string"},
            {"name": "state", "type": "string"},
            {"name": "date_time", "type": "int64"},
            {"name": "extra", "type": "string"}
        ]
    }
    schema_json = os.getenv("QUERY_RESULT_SCHEMA", json.dumps(default_schema))

    print("\n" + "=" * 40)
    print(" WoolMilk Processing Node Parameters")
    print("=" * 40)
    print(f" Port           : {port}")
    print(f" Forward Node   : {forward_node}")
    print(f" SQL Query      : {sql_query}")
    print(f" Schema         : {schema_json}")
    print("=" * 40 + "\n")

    processing_node = ProcessingNode(
        f"grpc://0.0.0.0:{port}", forward_node, sql_query, schema_json
    )
    print(f"WoolMilk processing node running on port {port}")
    processing_node.serve()