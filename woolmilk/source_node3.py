"""""
Source Node, that uses Nexmark to generate and stream Data
"""""

import argparse
import json
import time
from typing import Iterator, Dict, Any

import pyarrow.flight

from nexmark_generator_old import nexmark_event_generator_fake_live

ERROR_FLAG = False
SHUTDOWN_FLAG = False


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WoolMilk Source Node")
    parser.add_argument(
        "--stream",
        choices=["bid", "auction", "person"],
        default="person",
        help="Stream type",
    )

    parser.add_argument(
        "--schema",
        type=str,
        default='{"fields": [{"name": "id", "type": "int64"}]}',
        help="JSON schema definition for the data (required)",
    )

    parser.add_argument(
        "--generator-executable", default="nexmark", help="Executable to generate data"
    )

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
        default=10**6,
    )

    parser.add_argument(
        "--batch-per-second",
        type=int,
        help="Number of Batches to send per second (Limit, -1 if uncapped)",
        default=-1,
    )

    args: argparse.Namespace = parser.parse_args()

    tuple_rate = args.tuple_per_batch * args.batch_per_second if args.batch_per_second > 0 else 0

    print("\n" + "=" * 40)
    print(" WoolMilk Source Node Parameters")
    print("=" * 40)
    print(f" Stream Type                : {args.stream}")
    print(f" Processing Nodes           : {args.processing_nodes}")
    print(f" Tuple per Batch            : {args.tuple_per_batch}")
    print(f" Batch Rate                 : {args.batch_per_second}")
    print(f" Tuple Rate (derived)       : {tuple_rate if tuple_rate > 0 else "uncapped"}/s")
    print("=" * 40 + "\n")
    return args


def batch_streamer(stream_generator: Iterator[Dict[str, Any]], tuples_per_batch: int,
                   batch_per_second: int, writers: list[pyarrow.flight.FlightStreamWriter], schema: pyarrow.Schema):
    batch_buffer = []
    current_second = int(time.time())  #1s
    batches_sent_this_second = 0

    try:
        while not SHUTDOWN_FLAG:
            try:
                event = next(stream_generator)
            except StopIteration:
                continue


            now = int(time.time()) #1s

            if now != current_second and batches_sent_this_second < batch_per_second:
                print("EDGE CASE: TO Slow to send Batch or create Data")

            if now != current_second:
                current_second = now
                print(f"Drop {batches_sent_this_second + 1} batch")
                # Tuple rate per Second
                # CPU / Memory
                #
                batches_sent_this_second = 0
                batch_buffer.clear()

            if 0 < batch_per_second <= batches_sent_this_second:
                continue

            batch_buffer.append(event)

            if len(batch_buffer) >= tuples_per_batch:
                rb: pyarrow.RecordBatch = pyarrow.RecordBatch.from_pylist(batch_buffer, schema=schema)

                for writer in writers:
                    if SHUTDOWN_FLAG:
                        return
                    try:
                        writer.write_batch(rb)
                    except Exception:
                        break
                batches_sent_this_second += 1
                batch_buffer.clear()

    except KeyboardInterrupt:
        raise SystemExit


def get_schema(object: dict[str, any]) -> pyarrow.Schema:
    temp_table: pyarrow.Table = pyarrow.Table.from_pylist([object])
    return temp_table.schema

def get_writers(nodes: str, schema: pyarrow.Schema) -> list[pyarrow.flight.FlightStreamWriter]:
    writers: list[pyarrow.flight.FlightStreamWriter] = []
    for address in nodes.split(","):
        client = pyarrow.flight.FlightClient(f"grpc://{address}")
        writer, _ = client.do_put(pyarrow.flight.FlightDescriptor.for_path("bandwidth-test"), schema)
        writers.append(writer)
    return writers


def _parse_schema(schema_json):
    schema_dict = json.loads(schema_json)
    fields = []
    for field in schema_dict.get("fields", []):
        field_name = field["name"]
        field_type = field["type"]

        if field_type == "int64":
            pa_type = pyarrow.int64()
        elif field_type == "string":
            pa_type = pyarrow.string()
        elif field_type == "float64":
            pa_type = pyarrow.float64()
        else:
            pa_type = pyarrow.string()

        fields.append(pyarrow.field(field_name, pa_type))

    return pyarrow.schema(fields)




if __name__ == '__main__':
    args = parse_arguments()

    # gen = nexmark_event_generator_live(args.stream, args.generator_executable)
    schema = _parse_schema(args.schema)
    gen = nexmark_event_generator_fake_live(
        args.stream, args.generator_executable, schema
    )
    writers: list[pyarrow.flight.FlightStreamWriter] = get_writers(args.processing_nodes, schema)


    try:
        batch_streamer(gen, args.tuple_per_batch, args.batch_per_second, writers, schema)

    except SystemExit:
        for writer in writers:
            writer.done_writing()

        raise SystemExit