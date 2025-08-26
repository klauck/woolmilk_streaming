from datafusion import column, literal, SessionContext
import pyarrow as pa
import pyarrow.flight
import sys
import time
import argparse
import json

class BandwidthTestServer(pa.flight.FlightServerBase):
    def __init__(self, location, exit_node, sql_query, schema_json):
        super().__init__(location)
        self.client = pa.flight.FlightClient(f"grpc://{exit_node}")
        self.query = sql_query
        self.default_table_name = "nexmark_data"
        
        if not schema_json:
            raise ValueError("Schema is mandatory. Please provide a valid schema.")
        
        self.predefined_schema = self._parse_schema(schema_json)
        if not self.predefined_schema:
            raise ValueError("Failed to parse the provided schema.")
    
    def _parse_schema(self, schema_json):
        try:
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
        except Exception as e:
            print(f"Error parsing schema: {e}")
            return None

    def do_put(self, context, descriptor, reader, writer):
        ctx = SessionContext()
        total_bytes = 0
        start = time.time()

        exit_writer, _ = self.client.do_put(
            pa.flight.FlightDescriptor.for_path(self.query or self.default_table_name),
            self.predefined_schema
        )
        
        for chunk in reader:
            batch = chunk.data
            ctx.register_record_batches(self.default_table_name, [[batch]])
            df = ctx.table(self.default_table_name)
            if self.query:
                result_df = ctx.sql(self.query)
            else:
                result_df = df
            for out_batch in result_df.collect():
                exit_writer.write_batch(out_batch)
                total_bytes += out_batch.nbytes
            ctx.deregister_table(self.default_table_name)

        exit_writer.done_writing()
        end = time.time()

        duration = end - start
        mbps = (total_bytes * 8) / (duration * 1024 * 1024)
        print(f"Received {total_bytes} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Arrow Flight Processing Node")
    parser.add_argument(
        "--server-address",
        type=str,
        default="localhost:8815",
        help="Address to run the Flight processing node on (host:port)"
    )
    parser.add_argument(
        "--exit_node",
        type=str,
        default="localhost:8820",
        help="Address of the exit Flight node (host:port)"
    )
    parser.add_argument(
        "--query",
        type=str,
        default="SELECT * FROM nexmark_data",
        help="Optional SQL query to run on incoming batches"
    )
    parser.add_argument(
        "--query_result_schema",
        type=str,
        required=True,
        help="JSON schema definition for the data (required)"
    )
    args = parser.parse_args()

    print("\n" + "="*40)
    print(" Arrow Flight Processing Node Parameters")
    print("="*40)       
    print(f" Address        : {args.server_address}")
    print(f" Exit Node      : {args.exit_node}")
    print(f" SQL Query      : {args.query}")
    print(f" Schema         : Provided and parsed successfully")
    print("="*40 + "\n")

    address = args.server_address
    exit_node = args.exit_node
    sql_query = args.query
    schema_json = args.query_result_schema

    server = BandwidthTestServer(f"grpc://{address}", exit_node, sql_query, schema_json)
    print(f"Flight processing node running at {address}")
    server.serve()
