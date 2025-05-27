import argparse
from typing import Iterator
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.flight as fl
import threading
import time

class EntryNode:
    def __init__(
        self,
        parquet_path: str,
        stream_name: str,
        processor_host: str = "127.0.0.1",
        processor_port: int = 8815,
        chunk_size: int = 100_000,
        read_type: str = "memory",
    ):
        if read_type not in ("memory", "disk"):
            raise ValueError("read_type must be 'memory' or 'disk'")
        self.parquet_path = parquet_path
        self.client = fl.FlightClient(f"grpc://{processor_host}:{processor_port}")
        self.descriptor = fl.FlightDescriptor.for_path(stream_name)
        self.chunk_size = chunk_size
        self.read_type = read_type

    def chunks(self) -> Iterator[pa.Table]:
        if self.read_type == "memory":
            table = (
                # read_dictionary=False to avoid reading dictionary-encoded columns
                # replace_schema_metadata(None) to remove metadata that may not be needed
                pq.read_table(self.parquet_path, read_dictionary=False)
                  .replace_schema_metadata(None)
            )
            for offset in range(0, table.num_rows, self.chunk_size):
                yield table.slice(offset, self.chunk_size).combine_chunks()
            return

        pf = pq.ParquetFile(self.parquet_path)

        buffer: pa.Table | None = None

        #  function to append batches to the buffer, to match the chunk size
        def append(batch: pa.Table) -> None:
            nonlocal buffer
            buffer = batch if buffer is None else pa.concat_tables([buffer, batch])

        for row_group_idx in range(pf.num_row_groups):
            append(pf.read_row_group(row_group_idx))

            while buffer.num_rows >= self.chunk_size:
                yield buffer.slice(0, self.chunk_size)
                buffer = buffer.slice(self.chunk_size)

        if buffer is not None and buffer.num_rows > 0:
            yield buffer

    def send_data_to_processor(self, table: pa.Table):
        writer, _ = self.client.do_put(self.descriptor, table.schema)
        writer.write_table(table)
        writer.close()
        print(f"[Entry] Sent chunk with {table.num_rows} rows")

    def run(self):
        total_rows = 0
        total_chunks = 0
        thread_at_a_time = 5
        no_threads = 0
        for idx, tbl in enumerate(self.chunks(), start=1):
            total_rows += tbl.num_rows
            total_chunks += 1
            
            processor_thread = threading.Thread(
                target=self.send_data_to_processor, args=(tbl,)
            )

            processor_thread.start()
            no_threads += 1

            if no_threads == thread_at_a_time:
                while threading.active_count() > 1:
                    time.sleep(1)
                no_threads = 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=" EntryNode")
    parser.add_argument("--parquet", help="Path to parquet file.", default="/Users/usamabintariq/Documents/GitHub/woolmilk_streaming/data/bids.parquet")
    parser.add_argument("--stream-name", default="bids")
    parser.add_argument("--read-type", choices=["memory", "disk"], default="memory")
    parser.add_argument("--chunk-size", type=int, default=50_000)
    parser.add_argument("--processor-host", default="127.0.0.1")
    parser.add_argument("--processor-port", type=int, default=8815)
    args = parser.parse_args()

    entry_node = EntryNode(
        parquet_path=args.parquet,
        stream_name=args.stream_name,
        processor_host=args.processor_host,
        processor_port=args.processor_port,
        chunk_size=args.chunk_size,
        read_type=args.read_type,
    )

    entry_node.run()
