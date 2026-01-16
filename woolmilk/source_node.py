import argparse
import time
from typing import Iterator

import pyarrow as pa
import pyarrow.flight as pf
import pyarrow.parquet as pq

SHUTDOWN_FLAG = False

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WoolMilk Source Node")
    parser.add_argument(
        "--processing-nodes",
        help="Flight server address (host:port,host:port)",
        type=str,
        default="localhost:8010",
    )

    parser.add_argument(
        "--tuple-per-batch",
        type=int,
        help="Number of Tuples to send per Batch",
        default=10**4,
    )

    parser.add_argument(
        "--batch-per-second",
        type=int,
        help="Number of Batches to send per second (Limit, -1 if uncapped)",
        default=-1,
    )

    parser.add_argument(
        "--overall_batches",
        type=int,
        help="Number of overall batches to send (Limit, -1 if uncapped)",
        default=-1,
    )

    parser.add_argument(
        "--file-path",
        type=str,
        default="data/test.parquet",
        help="Path to the Parquet file containing Nexmark data",
    )
    args: argparse.Namespace = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Source Node Parameters")
    print("=" * 40)
    print(f" Processing Nodes           : {args.processing_nodes}")
    print(f" Overall Batches            : {args.overall_batches}")
    print(f" Batch per Second           : {args.batch_per_second}")
    print(f" Tuple per Batch            : {args.tuple_per_batch}")
    print(f" Parquet File               : {args.file_path}")
    print("=" * 40 + "\n")
    return args

def add_timestamp(batch: pa.RecordBatch) -> pa.RecordBatch:
    now = time.time()
    timestamps = pa.array([now] * batch.num_rows, type=pa.int64())
    return batch.append_column("timestamp", timestamps)

def parse_parquet_file(parquet_file: pq.ParquetFile, overall_batches: int, tuple_per_batch: int) -> Iterator[pa.RecordBatch]:
    global SHUTDOWN_FLAG
    current_batch_count = 0
    while not SHUTDOWN_FLAG:
        for batch in parquet_file.iter_batches(batch_size=tuple_per_batch):
            if SHUTDOWN_FLAG:
                return

            if overall_batches != -1:
                if current_batch_count >= overall_batches:
                    SHUTDOWN_FLAG = True
                    return
                current_batch_count += 1

            yield add_timestamp(batch)

def get_writers(nodes: str, schema: pa.Schema) -> list[pf.FlightStreamWriter]:
    writers: list[pf.FlightStreamWriter] = []
    for address in nodes.split(","):
        client = pf.FlightClient(f"grpc://{address}")
        writer, _ = client.do_put(pf.FlightDescriptor.for_path("bandwidth-test"), schema)
        writers.append(writer)
    return writers

def batch_streamer(stream_generator: Iterator[pa.RecordBatch], batch_per_second: int,
                   writers: list[pf.FlightStreamWriter]):

    global SHUTDOWN_FLAG

    start_time = time.time()
    current_second = int(start_time)
    batches_sent_this_second = 0
    counter = -1

    try:
        while not SHUTDOWN_FLAG:

            now = int(time.time())

            if now != current_second:
                print(f"{batches_sent_this_second} were sent in a second.")
                current_second = now
                batches_sent_this_second = 0



            if batch_per_second != -1 and batches_sent_this_second >= batch_per_second:
                time.sleep(0.005)
                continue

            try:
                batch: pa.RecordBatch = next(stream_generator)
            except StopIteration:
                print("Sent all Batches provided by generator")
                break

            counter = (counter + 1) % len(writers)
            if SHUTDOWN_FLAG:
                return
            try:
                writers[counter].write_batch(batch)
            except Exception as e:
                print(f"Got exception while trying to send batch: {e}")
                SHUTDOWN_FLAG = True
                break

            batches_sent_this_second += 1

    finally:
        for writer in writers:
            writer.done_writing()
        end_time = time.time()
        print(f"Start: {start_time} and End: {end_time} with total duration: {end_time - start_time} seconds")

if __name__ == "__main__":
    args = parse_arguments()

    parquet_file: pq.ParquetFile = pq.ParquetFile(args.file_path)
    schema: pa.Schema = parquet_file.schema_arrow
    schema = schema.append(pa.field("timestamp", pa.int64()))
    print(schema)

    gen = parse_parquet_file(parquet_file, args.overall_batches, args.tuple_per_batch)
    writers: list[pf.FlightStreamWriter] = get_writers(args.processing_nodes, schema)

    try:
        batch_streamer(gen, args.batch_per_second, writers)
    except KeyboardInterrupt:
        SHUTDOWN_FLAG = True
        for writer in writers:
            writer.done_writing()




