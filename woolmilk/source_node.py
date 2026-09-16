import argparse
import json
import subprocess
import sys
import threading
import time
from fractions import Fraction
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import flight

from woolmilk.encoding import (
    dictionary_encode_batch,
    get_compressed_flight_options,
    set_schema_encoding,
)
from woolmilk.rate_profile import build_rate_profile_per_thread, current_interval


class NodeStatus:
    IDLE = "IDLE"
    GENERATING_DATA = "GENERATING_DATA"
    DATA_GENERATED = "DATA_GENERATED"
    SENDING_DATA = "SENDING_DATA"
    RECEIVING_DATA = "RECEIVING_DATA"


class SourceNodeActions:
    GENERATE_DATA = "GENERATE_DATA"
    SEND_DATA = "SEND_DATA"
    GET_STATUS = "GET_STATUS"


def scheduled_timestamps(base_ms, sched_elapsed, interval, n):
    """int64 event timestamps (ms) for one batch: `base_ms` shifted by the
    scheduled elapsed time, with the `n` tuples spread across the batch's
    `interval` so their density follows the rate profile (dense during bursts,
    sparse during baseline). `interval` None -> all tuples share `sched_elapsed`.
    """
    step = (interval / n) if interval else 0.0
    offsets_s = sched_elapsed + np.arange(n) * step
    return (base_ms + offsets_s * 1000.0).astype(np.int64)


def _set_date_time(batch, ts_array):
    """Return a copy of `batch` with its `date_time` column replaced."""
    idx = batch.schema.names.index("date_time")
    field_type = batch.schema.field(idx).type
    arrays = [batch.column(i) for i in range(batch.num_columns)]
    arrays[idx] = pa.array(ts_array, type=field_type)
    return pa.RecordBatch.from_arrays(arrays, schema=batch.schema)


class SourceNode(flight.FlightServerBase):
    def __init__(
        self,
        location,
        processing_nodes,
        overall_tuples,
        tuples_per_batch,
        stream,
        generator_executable,
        offset,
        step,
        input_folder,
        store_input,
        experiment_id,
        iteration_id,
        source_node_id,
        batches_per_second=None,
        compression=None,
        encoding=None,
        columns_to_encode=None,
        rate_profile=None,
        rewrite_timestamps=True,
    ):
        super().__init__(location)
        self.location = location
        self.current_status = NodeStatus.IDLE
        self.processing_nodes = processing_nodes
        self.overall_tuples = overall_tuples
        self.tuples_per_batch = tuples_per_batch
        self.stream = stream
        self.generator_executable = generator_executable
        self.offset = offset
        self.step = step
        self.input_folder = input_folder
        self.store_input = store_input
        self.experiment_id = experiment_id
        self.iteration_id = iteration_id
        self.source_node_id = source_node_id
        self.batches_per_second_per_thread = (
            batches_per_second / len(processing_nodes) if batches_per_second else None
        )
        self.batches_per_second = batches_per_second
        self.compression = compression
        self.encoding = encoding
        self.columns_to_encode = columns_to_encode
        # Time-varying send rate (temporal bursts). Falls back to a single
        # constant phase derived from `batches_per_second`, else None
        # (send as fast as possible).
        self.rate_profile_per_thread = build_rate_profile_per_thread(
            rate_profile, batches_per_second, len(processing_nodes)
        )
        self.rewrite_timestamps = rewrite_timestamps
        self.thread_tables = [None] * len(processing_nodes)
        self.threads = []
        self.completed_threads = 0
        self.lock = threading.Lock()

    def start(self):
        print(f"WoolMilk source node running at {self.location}")
        self.serve()

    def do_action(self, context, action):
        if action.type == SourceNodeActions.GET_STATUS:
            yield flight.Result(self.current_status.encode("utf-8"))
        elif action.type == SourceNodeActions.GENERATE_DATA:
            self.current_status = NodeStatus.GENERATING_DATA
            self.generate_data()
            yield flight.Result(self.current_status.encode("utf-8"))
        elif action.type == SourceNodeActions.SEND_DATA:
            self.completed_threads = 0
            self.current_status = NodeStatus.SENDING_DATA
            self.start_streaming()
            yield flight.Result(self.current_status.encode("utf-8"))

    def generate_data(self):
        tuples_per_thread = self.overall_tuples // len(self.processing_nodes)

        for thread_id in range(len(self.processing_nodes)):
            thread_offset = self.offset + thread_id
            path = (
                Path(self.input_folder)
                / f"{self.stream}_{tuples_per_thread}_{thread_offset}_{self.step}.parquet"
            )

            if path.exists():
                parquet_file = pq.ParquetFile(path)
                self.thread_tables[thread_id] = parquet_file.read()
            else:
                table = generate_table(
                    number_of_tuples=tuples_per_thread,
                    stream=self.stream,
                    generator_executable=self.generator_executable,
                    offset=thread_offset,
                    step=self.step,
                )
                if self.store_input:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    pq.write_table(
                        table,
                        path,
                        row_group_size=self.tuples_per_batch,
                        compression="snappy",
                    )
                    print(f"Wrote .. {path}")
                self.thread_tables[thread_id] = table

        self.current_status = NodeStatus.DATA_GENERATED

    def start_streaming(self):
        if any(t is None for t in self.thread_tables):
            raise RuntimeError("Data not generated. Call GENERATE_DATA action first.")

        self.threads = []
        for thread_id in range(len(self.processing_nodes)):
            table = self.thread_tables[thread_id]
            assert table is not None
            t = threading.Thread(
                target=self.stream_data,
                args=(
                    thread_id,
                    table.schema,
                    table.to_batches(max_chunksize=self.tuples_per_batch),
                    self.processing_nodes[thread_id],
                    self.experiment_id,
                    self.iteration_id,
                    self.source_node_id,
                ),
                daemon=True,
            )
            self.threads.append(t)
            t.start()

    def stream_data(
        self,
        thread_id,
        schema,
        batches,
        processing_node,
        experiment_id=None,
        iteration_id=None,
        source_node_id=None,
    ):
        print(f"SEND_TIME: {time.time()}")
        path_info = {
            "experiment_id": experiment_id,
            "iteration_id": iteration_id,
            "source_node_id": source_node_id,
            "thread_id": thread_id,
        }
        encoded_path = json.dumps(path_info)

        client = flight.FlightClient(f"grpc://{processing_node[0]}:{processing_node[1]}")

        use_dictionary_encoding = self.encoding == "dictionary"
        target_schema = schema
        if use_dictionary_encoding:
            assert self.columns_to_encode is not None
            target_schema = set_schema_encoding(schema, self.columns_to_encode)

        call_options = None
        if self.compression:
            call_options = get_compressed_flight_options(codec=self.compression)

        writer, _ = client.do_put(
            flight.FlightDescriptor.for_path(encoded_path),
            target_schema,
            options=call_options,
        )

        start = time.time()
        send_times = []
        total_bytes = 0
        next_send_time = start
        # Only rewrite event timestamps when there is an actual rate schedule to
        # match; a plain run keeps Nexmark's original date_time values.
        rewrite = (
            self.rewrite_timestamps
            and self.rate_profile_per_thread is not None
            and "date_time" in schema.names
        )
        base_ms = None

        for i, batch in enumerate(batches):
            # Pace according to the (possibly time-varying) rate profile. This
            # also covers a constant `batches_per_second`, which is folded into
            # the profile as a single phase.
            sched_elapsed = next_send_time - start
            interval = current_interval(self.rate_profile_per_thread, sched_elapsed)
            if interval is not None:
                now = time.time()
                if next_send_time > now:
                    time.sleep(next_send_time - now)
                send_start = time.time()
                next_send_time += interval
            else:
                send_start = time.time()
                next_send_time = send_start

            if rewrite and batch.num_rows:
                if base_ms is None:
                    dt_idx = schema.names.index("date_time")
                    base_ms = int(batch.column(dt_idx).to_numpy().min())
                ts = scheduled_timestamps(base_ms, sched_elapsed, interval, batch.num_rows)
                batch = _set_date_time(batch, ts)

            if use_dictionary_encoding:
                assert self.columns_to_encode is not None
                batch = dictionary_encode_batch(batch, self.columns_to_encode)

            batch_id = f"{source_node_id}:{thread_id}:{i}"
            writer.write_with_metadata(batch, batch_id.encode("utf-8"))
            batch_bytes = batch.nbytes
            total_bytes += batch_bytes
            send_end = time.time()
            send_times.append((send_start, send_end, batch_id, batch_bytes))
        writer.done_writing()

        end = time.time()

        duration = end - start
        gbps = (total_bytes * 8) / (duration * 1000**3)
        mbps = total_bytes / (duration * 1000**2)

        log = {
            "thread": thread_id,
            "experiment_id": experiment_id,
            "iteration_id": iteration_id,
            "source_node_id": source_node_id,
            "send_times": send_times,
            "total_bytes": total_bytes,
            "start_time": start,
            "end_time": end,
            "gbps": f"{gbps:.4f}",
            "mbps": f"{mbps:.2f}",
        }

        print(f"WM_LOG= {json.dumps(log)}")

        with self.lock:
            self.completed_threads += 1
            if self.completed_threads == len(self.processing_nodes):
                self.current_status = NodeStatus.IDLE


def generate_table(
    number_of_tuples=10**6,
    stream="nexmark_person",
    generator_executable="nexmark",
    offset=0,
    step=1,
):
    assert (
        len(stream.split("_")) == 2 and stream.split("_")[0] == "nexmark"
    ), f"stream must be in format 'nexmark_<event_type>', got: {stream}"
    event_type = stream.split("_")[1]
    cmd = [
        generator_executable,
        "-n",
        str(number_of_tuples),
        "--offset",
        str(offset),
        "--step",
        str(step),
        "--type",
        event_type,
        "--no-wait",
    ]
    print("Generate data..")
    print(f"  {event_type}   #tuples: {number_of_tuples}  offset: {offset}  step: {step}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    assert proc.stdout is not None
    records = []

    try:
        for line in proc.stdout:
            try:
                record = json.loads(line)
                key = event_type.capitalize()
                records.append(record[key])
            except json.JSONDecodeError:
                continue
    finally:
        proc.stdout.close()
        proc.kill()
        proc.wait()

    print("Done.")
    return pa.Table.from_pylist(records)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Source Node")
    parser.add_argument(
        "--input-folder",
        default="input_data",
        help="Input Parquet files or folder to store generated data",
    )
    parser.add_argument(
        "--stream",
        choices=["nexmark_bid", "nexmark_auction", "nexmark_person"],
        default="nexmark_person",
        help="Stream type",
    )
    parser.add_argument(
        "--generator-executable", default="nexmark", help="Executable to generate data"
    )
    parser.add_argument(
        "--overall-tuples",
        type=int,
        default=10**5,
        help="Total number of tuples needs to be sent.",
    )
    parser.add_argument(
        "--tuples-per-batch", type=int, default=10**4, help="Number of tuples per batch"
    )
    parser.add_argument(
        "--offset", type=int, default=0, help="Offset to start data generation"
    )
    parser.add_argument(
        "--step", type=int, default=-1, help="Step for next tuple to generate"
    )
    parser.add_argument(
        "--processing-nodes",
        help="Flight server address (host:port,host:port)",
        type=str,
        default="localhost:8010",
    )
    parser.add_argument(
        "-store-input",
        action="store_true",
        help="Store generated data",
    )
    parser.add_argument(
        "--experiment-id",
        type=int,
        default=None,
        help="Experiment ID for logging metadata",
    )
    parser.add_argument(
        "--iteration-id",
        type=int,
        default=None,
        help="Iteration ID for logging metadata",
    )
    parser.add_argument(
        "--source-node-id",
        type=int,
        default=None,
        help="Unique ID for the source node",
    )
    parser.add_argument(
        "--source-server-address",
        type=str,
        default=None,
        help="Address where source flight server starts",
    )
    parser.add_argument(
        "--batches-per-second",
        type=str,
        default=None,
        help="Number of batches sent per second (e.g. 1.5 or 3/2)",
    )
    parser.add_argument(
        "--compression",
        type=str,
        choices=["zstd", "lz4"],
        default=None,
        help="Compression codec for outbound Flight payload",
    )
    parser.add_argument(
        "--encoding",
        type=str,
        choices=["dictionary"],
        default=None,
        help="Encoding applied to string columns before transmission",
    )
    parser.add_argument(
        "--columns-to-encode",
        type=str,
        default=None,
        help="Comma-separated columns to dictionary-encode (e.g. city,name)",
    )
    parser.add_argument(
        "--rate-profile",
        type=str,
        default=None,
        help=(
            "JSON describing a time-varying send rate, e.g. to simulate "
            'temporal bursts: \'{"phases": '
            '[{"duration_s": 10, "batches_per_second": 2}, '
            '{"duration_s": 5, "batches_per_second": 50}], "loop": true}\'. '
            "Overrides --batches-per-second if set."
        ),
    )
    parser.add_argument(
        "--no-rewrite-timestamps",
        dest="rewrite_timestamps",
        action="store_false",
        help="Do not rewrite tuple date_time to match the rate profile "
        "(default: rewrite so event timestamps follow the burst pattern).",
    )
    args = parser.parse_args()

    processing_nodes = []
    for address in args.processing_nodes.split(","):
        host, port = address.split(":")
        processing_nodes.append((host, int(port)))

    if args.step == -1:
        args.step = len(processing_nodes)

    batches_per_second = None
    if args.batches_per_second:
        batches_per_second = Fraction(args.batches_per_second)
        assert batches_per_second > 0, "batches_per_second must be positive"

    columns_to_encode = (
        args.columns_to_encode.split(",") if args.columns_to_encode else None
    )

    rate_profile = None
    if args.rate_profile:
        rate_profile = json.loads(args.rate_profile)
        assert "phases" in rate_profile and rate_profile["phases"], (
            "rate_profile must contain a non-empty 'phases' list"
        )

    print("\n" + "=" * 40)
    print(" WoolMilk Source Node Parameters")
    print("=" * 40)
    print(f" Input Folder               : {args.input_folder}")
    print(f" Stream Type                : {args.stream}")
    print(f" Overall Tuples             : {args.overall_tuples}")
    print(f" Tuples Per Batch           : {args.tuples_per_batch}")
    print(f" Offset                     : {args.offset}")
    print(f" Step                       : {args.step}")
    print(f" Store Input                : {args.store_input}")
    print(f" Processing Nodes           : {args.processing_nodes}")
    print(f" Generator Executable       : {args.generator_executable}")
    print(f" Experiment ID              : {args.experiment_id}")
    print(f" Iteration ID               : {args.iteration_id}")
    print(f" Source Node ID             : {args.source_node_id}")
    print(f" Source Server Address      : {args.source_server_address}")
    print(f" Batches Per Second         : {batches_per_second}")
    print(f" Compression                : {args.compression}")
    print(f" Encoding                   : {args.encoding}")
    print(f" Columns to Encode          : {args.columns_to_encode}")
    print(f" Rate Profile               : {rate_profile}")
    print("=" * 40 + "\n")

    if len(processing_nodes) == 0:
        print("No server addresses for processing nodes provided. Exiting.")
        sys.exit(1)

    assert args.overall_tuples % (args.tuples_per_batch * len(processing_nodes)) == 0, (
        f"overall_tuples ({args.overall_tuples}) must be divisible by "
        f"tuples_per_batch ({args.tuples_per_batch}) * "
        f"number of processing nodes ({len(processing_nodes)})"
    )

    source_node = SourceNode(
        location=(
            f"grpc://{args.source_server_address}" if args.source_server_address else None
        ),
        processing_nodes=processing_nodes,
        overall_tuples=args.overall_tuples,
        tuples_per_batch=args.tuples_per_batch,
        stream=args.stream,
        generator_executable=args.generator_executable,
        offset=args.offset,
        step=args.step,
        input_folder=args.input_folder,
        store_input=args.store_input,
        experiment_id=args.experiment_id,
        iteration_id=args.iteration_id,
        source_node_id=args.source_node_id,
        batches_per_second=batches_per_second,
        compression=args.compression,
        encoding=args.encoding,
        columns_to_encode=columns_to_encode,
        rate_profile=rate_profile,
        rewrite_timestamps=args.rewrite_timestamps,
    )

    if args.source_server_address:
        source_node.start()
    else:
        print("Generating data...")
        source_node.generate_data()
        print("Streaming data...")
        source_node.start_streaming()

        while source_node.current_status != NodeStatus.IDLE:
            print("Waiting for threads to finish...")
            time.sleep(1)
