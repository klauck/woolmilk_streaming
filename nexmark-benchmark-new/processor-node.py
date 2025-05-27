from __future__ import annotations
import argparse
import pyarrow as pa
import pyarrow.flight as fl
from datafusion import SessionContext
import threading

class ProcessorFlightServer(fl.FlightServerBase):
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8815,
        exit_host: str = "0.0.0.0",
        exit_port: int = 8915
    ) -> None:
        location = f"grpc://{host}:{port}"
        self.exit_client = fl.FlightClient(f"grpc://{exit_host}:{exit_port}")
        self.process_index = 0
        super().__init__(location)

    def do_put(self, context, descriptor, reader, writer) -> None:
        self.process_index += 1
        index = self.process_index
        path = descriptor.path[0].decode() if descriptor.path else "bids"
        print(f"[Processor[{index}]] Received data for stream:", path)

        batches = []
        for batch_no, batch in enumerate(reader, start=1):
            batches.append(batch.data)

        table = pa.Table.from_batches(batches)
        process_data_thread = threading.Thread(
            target=self.process_data,
            args=(table, path, index)
        )
        print(f"[Processor[{index}]] Processing data for stream '{path}' with {table.num_rows} rows")
        process_data_thread.start()

    def process_data(self, table: pa.Table, stream_name: str, index: int) -> None:
        ctx = SessionContext()

        df = table.to_pandas() # unnecessary conversion to pandas DataFrame, because of memory alignment issues with DataFusion
        try:
            ctx.from_pandas(df, stream_name)
            
            df = ctx.sql(f"SELECT * FROM {stream_name}")
            result_table = df.to_arrow_table()

            print(f"[Processor[{index}]] Query result for '{stream_name}': {result_table.num_rows} rows total")
            self.pass_to_exit_node(
                fl.FlightDescriptor.for_path(stream_name),
                result_table,
                index
            )
        except Exception as e:
            print(f"[Processor[{index}]] Error converting table to DataFusion: {e}")
            return
        
    def pass_to_exit_node(self, descriptor: fl.DescriptorType, table: pa.Table, index: int) -> None:
        print(f"[Processor[{index}]] Passing data to exit node...")
        writer, _ = self.exit_client.do_put(descriptor, table.schema)
        writer.write_table(table)
        writer.close()
        print(f"[Processor[{index}]] Data passed to exit node successfully.")
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simple ProcessorFlightServer that prints batch stats"
    )
    parser.add_argument(
        "--host", default="0.0.0.0",
        help="Host to bind on (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", type=int, default=8815,
        help="Port to bind on (default: 8815)"
    )
    args = parser.parse_args()

    server = ProcessorFlightServer(host=args.host, port=args.port)
    print(f"ProcessorFlightServer listening on grpc://{args.host}:{args.port}")
    server.serve()