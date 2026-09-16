import argparse
import json
import queue
import threading
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from datafusion import SessionConfig, SessionContext
from pyarrow import flight

from woolmilk.encoding import (
    dictionary_decode_batch,
    dictionary_encode_batch,
    get_compressed_flight_options,
    set_schema_encoding,
)
from woolmilk.runtime_config import RuntimeConfig
from woolmilk.source_node import NodeStatus, SourceNodeActions


class ProcessingNodeActions:
    SET_CONFIG = "SET_CONFIG"
    GET_LOGS = "get_logs"
    DELETE_LOGS = "delete_logs"


class ProcessingNode(flight.FlightServerBase):
    def __init__(self, location, forward_node, runtime_config=None):
        super().__init__(location)
        self.forwarding_client = flight.FlightClient(f"grpc://{forward_node}")
        self.default_table_name = "nexmark_data"
        self.logs = []
        self.logs_lock = threading.Lock()
        self.open_requests = 0
        self.open_requests_lock = threading.Lock()

        self.runtime_config = runtime_config or RuntimeConfig()

        self.predefined_schema = None
        if self.runtime_config.query_result_schema:
            self.predefined_schema = self.parse_schema(
                json.dumps(self.runtime_config.query_result_schema)
            )
            print(self.predefined_schema)

        # Only used for windowed queries
        # Slide state keyed by slide start
        self.state = {}
        # Track sources with their status and watermarks
        self.active_sources = {}
        # Pending window starts to flush.
        self.pending_windows = set()
        self.windowing_lock = threading.Lock()

    def parse_schema(self, schema_json):
        schema_dict = json.loads(schema_json)
        fields = []
        for field in schema_dict.get("fields", []):
            field_name = field["name"]
            field_type = field["type"]
            field_nullability = field["nullable"] if "nullable" in field else True

            if field_type == "int64":
                pa_type = pa.int64()
            elif field_type == "string":
                pa_type = pa.string()
            elif field_type == "float64":
                pa_type = pa.float64()
            else:
                pa_type = pa.string()

            fields.append(pa.field(field_name, pa_type, field_nullability))

        return pa.schema(fields)

    def do_action(self, context, action):
        if action.type == ProcessingNodeActions.GET_LOGS:
            with self.logs_lock:
                logs = self.logs
            yield flight.Result(json.dumps(logs).encode("utf-8"))
        elif action.type == ProcessingNodeActions.DELETE_LOGS:
            with self.logs_lock:
                self.logs = []
        elif action.type == ProcessingNodeActions.SET_CONFIG:
            try:
                cfg = RuntimeConfig.from_json(bytes(action.body.to_pybytes()))
            except Exception as e:
                yield flight.Result(f"ERR:{e}".encode("utf-8"))
                return
            self.runtime_config = cfg
            if cfg.query_result_schema is not None:
                self.predefined_schema = self.parse_schema(
                    json.dumps(cfg.query_result_schema)
                )
            print(f"SET_CONFIG applied: {cfg}")
            yield flight.Result(b"OK")
        elif action.type == SourceNodeActions.GET_STATUS:
            with self.open_requests_lock:
                if self.open_requests == 0:
                    status = NodeStatus.IDLE
                else:
                    status = NodeStatus.SENDING_DATA
            yield flight.Result(status.encode("utf-8"))
        else:
            raise NotImplementedError(f"Unknown action: {action.type}")

    def _split_batch(
        self, batch: pa.RecordBatch, window_slide
    ) -> dict[int, list[pa.RecordBatch]]:
        # Group tuples into slides based on the timestamp
        # to avoid duplicating tuples into overlapping windows.
        slides = {}
        if batch.num_rows == 0:
            return slides

        timestamps = batch.column("date_time")
        timestamps_np = timestamps.to_numpy(zero_copy_only=True)
        slide_starts_np = (timestamps_np // window_slide) * window_slide
        print(slide_starts_np)

        change_idx = np.flatnonzero(slide_starts_np[1:] != slide_starts_np[:-1]) + 1
        print(change_idx)
        start_idx = 0
        for end_idx in change_idx:
            sub_batch = batch.slice(start_idx, end_idx - start_idx)
            slides.setdefault(int(slide_starts_np[start_idx]), []).append(sub_batch)
            start_idx = end_idx

        sub_batch = batch.slice(start_idx, len(slide_starts_np) - start_idx)
        slides.setdefault(int(slide_starts_np[start_idx]), []).append(sub_batch)

        return slides

    def _assign_windows_to_timestamp(
        self, timestamp: int, window_size, window_slide
    ) -> list[int]:
        # Compute all windows an event belongs to
        windows = []

        latest_start = (timestamp // window_slide) * window_slide

        num_windows = (window_size + window_slide - 1) // window_slide
        for i in range(num_windows):
            window_start = latest_start - (i * window_slide)
            if window_start <= timestamp < window_start + window_size:
                windows.append(window_start)
            else:
                break

        return windows

    def _register_pending_windows(self, slide_starts, window_size, window_slide) -> None:
        for slide_start in slide_starts:
            for window_start in self._assign_windows_to_timestamp(
                slide_start, window_size, window_slide
            ):
                self.pending_windows.add(window_start)

    def _filter_batches_before(self, batches, window_end):
        filtered = []
        for batch in batches:
            timestamps = batch.column("date_time")
            mask = pc.less(timestamps, window_end)
            filtered_batch = batch.filter(mask)
            if filtered_batch.num_rows:
                filtered.append(filtered_batch)
        return filtered

    def _materialize_window_batches(self, window_end, window_slides, window_slide):
        batches = []
        for slide_start, slide_batches in window_slides:
            if slide_start + window_slide <= window_end:
                batches.extend(slide_batches)
            else:
                batches.extend(self._filter_batches_before(slide_batches, window_end))
        return batches

    def _flush_window(self, batches: list[pa.RecordBatch], forward_writer):
        # Run query on materialized window once it's ready and forward the results
        if not batches:
            return
        ctx = SessionContext()
        ctx.register_record_batches("nexmark_data", partitions=[batches])
        df = ctx.sql(self.query)
        for batch in df.collect():
            forward_writer.write_batch(batch.cast(self.predefined_schema))
        ctx.deregister_table("nexmark_data")

    def _collect_windows_to_flush(self, window_size, window_slide):
        # Get completed windows by comparing to the global watermark
        to_flush = []
        active_watermarks = [
            src["watermark"]
            for src in self.active_sources.values()
            if not src["is_finished"]
        ]

        if not active_watermarks:
            # All sources are finished; flush all remaining windows
            ready_windows = sorted(self.pending_windows)
            global_watermark = None
        else:
            global_watermark = min(active_watermarks)
            ready_windows = sorted(
                window_start
                for window_start in self.pending_windows
                if window_start + window_size <= global_watermark
            )

        for window_start in ready_windows:
            window_end = window_start + window_size
            window_slides = []
            slide_start = window_start
            while slide_start < window_end:
                slide_batches = self.state.get(slide_start)
                if slide_batches:
                    window_slides.append((slide_start, slide_batches))
                slide_start += window_slide

            if window_slides:
                to_flush.append((window_end, window_slides))
            self.pending_windows.discard(window_start)

        if global_watermark is None:
            self.state.clear()
        else:
            expired_slides = [
                slide_start
                for slide_start in self.state.keys()
                if slide_start + window_size <= global_watermark
            ]
            for slide_start in expired_slides:
                self.state.pop(slide_start, None)

        return to_flush

    def do_put(self, context, descriptor, reader, writer):
        cfg = self.runtime_config
        if not cfg.query:
            raise flight.FlightServerError(
                "Invalid runtime config: 'query' is unset. "
                "Send SET_CONFIG with a 'query' field before sending data."
            )
        if self.predefined_schema is None:
            raise flight.FlightServerError(
                "Invalid runtime config: 'query_result_schema' is unset. "
                "Send SET_CONFIG with a 'query_result_schema' before sending data."
            )

        with self.open_requests_lock:
            self.open_requests += 1

        ctx = SessionContext(SessionConfig().with_batch_size(cfg.tuples_per_batch))

        # set runtime configuration for connection
        query: str = cfg.query
        if cfg.windowing:
            windowing = True
            window_size = cfg.windowing["window_size"]
            window_slide = cfg.windowing["window_slide"]
        else:
            windowing = False
        use_dictionary_encoding = cfg.encoding == "dictionary"
        target_schema = self.predefined_schema
        if use_dictionary_encoding:
            assert cfg.columns_to_encode is not None
            target_schema = set_schema_encoding(target_schema, cfg.columns_to_encode)

        call_options = None
        if cfg.compression:
            call_options = get_compressed_flight_options(codec=cfg.compression)

        # Get and set benchmarking information
        experiment_id = None
        iteration_id = None
        source_node_id = None
        thread_id = None

        try:
            incoming_path_info = json.loads(descriptor.path[0].decode("utf-8"))
            if isinstance(incoming_path_info, dict):
                experiment_id = incoming_path_info.get("experiment_id")
                iteration_id = incoming_path_info.get("iteration_id")
                source_node_id = incoming_path_info.get("source_node_id")
                thread_id = incoming_path_info.get("thread_id")
        except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
            pass

        if windowing:
            with self.windowing_lock:
                assert source_node_id is not None and thread_id is not None
                source_key = (source_node_id, thread_id)
                self.active_sources[source_key] = {"watermark": 0, "is_finished": False}

        forwarded_path_info = json.dumps(
            {
                "experiment_id": experiment_id,
                "iteration_id": iteration_id,
                "source_node_id": source_node_id,
                "thread_id": thread_id,
            }
        )

        forward_writer, _ = self.forwarding_client.do_put(
            flight.FlightDescriptor.for_path(forwarded_path_info),
            schema=target_schema,
            options=call_options,
        )

        input_bytes = 0
        output_bytes = 0
        input_rows = 0
        output_rows = 0
        forwarding_times = []
        cost_break_down = {
            "receiving": [],
            "decoding": [],
            "querying": [],
            "encoding": [],
            "sending": [],
            "queue_wait": [],
        }
        start = time.time()

        def process_batch(batch, batch_in_hand_t, incoming_metadata):
            nonlocal input_bytes, output_bytes, input_rows, output_rows

            metadata = (
                json.loads(bytes(incoming_metadata).decode("utf-8"))
                if incoming_metadata
                else None
            )
            if metadata:
                batch_id = metadata.get("batch_id")
                watermark = metadata.get("watermark")

            decoding_start = time.time()
            if use_dictionary_encoding:
                batch = dictionary_decode_batch(batch)
            decoding_end = time.time()

            input_bytes += batch.nbytes
            input_rows += batch.num_rows

            print("\n\n\nprocess batch", batch_id, watermark, window_size, window_slide)
            querying_start = time.time()
            if windowing:
                print(batch)
                slide_batches = self._split_batch(batch, window_slide)
                print("slide_batches", slide_batches)
                print("get lock")
                with self.windowing_lock:
                    self.active_sources[source_key]["watermark"] = watermark
                    for slide_start, sub_batches in slide_batches.items():
                        self.state.setdefault(slide_start, []).extend(sub_batches)
                    print("register pending batches")
                    self._register_pending_windows(
                        slide_batches.keys(), window_size, window_slide
                    )
                    to_flush = self._collect_windows_to_flush(window_size, window_slide)
                    print("flush", to_flush)
                    result = []
                    for window_end, window_slides in to_flush:
                        batches = self._materialize_window_batches(
                            window_end, window_slides, window_slide
                        )
                        if not batches:
                            continue
                        ctx.register_record_batches(self.default_table_name, [batches])
                        result_df = ctx.sql(query)
                        result.extend(result_df.collect())
                        print("result", result)
                        ctx.deregister_table(self.default_table_name)

            else:
                print(batch)
                ctx.register_record_batches(self.default_table_name, [[batch]])
                result_df = ctx.sql(query)
                result = result_df.collect()
                ctx.deregister_table(self.default_table_name)

            querying_end = time.time()
            encoding_total = 0.0
            sending_total = 0.0
            batch_output_bytes = 0
            for j, result_batch in enumerate(result):
                output_rows += result_batch.num_rows
                if use_dictionary_encoding:
                    assert cfg.columns_to_encode is not None
                    enc_start = time.time()
                    result_batch = dictionary_encode_batch(
                        result_batch, cfg.columns_to_encode
                    )
                    encoding_total += time.time() - enc_start
                outgoing_id = (
                    batch_id
                    if batch_id is None or len(result) == 1
                    else f"{batch_id}.{j}"
                )
                send_start = time.time()
                if outgoing_id is not None:
                    forward_writer.write_with_metadata(
                        result_batch, outgoing_id.encode("utf-8")
                    )
                else:
                    forward_writer.write_batch(result_batch)
                sending_total += time.time() - send_start
                output_bytes += result_batch.nbytes
                batch_output_bytes += result_batch.nbytes

            forward_end = time.time()

            forwarding_times.append(
                (batch_in_hand_t, forward_end, batch_id, batch_output_bytes)
            )
            cost_break_down["decoding"].append(decoding_end - decoding_start)
            cost_break_down["querying"].append(querying_end - querying_start)
            cost_break_down["encoding"].append(encoding_total)
            cost_break_down["sending"].append(sending_total)

        if cfg.use_buffering:
            print("using buffering...")
            q = queue.Queue()

            def processing_worker():
                while True:
                    wait_start = time.time()
                    item = q.get()
                    wait_end = time.time()
                    if item is None:
                        break
                    batch, metadata = item
                    cost_break_down["queue_wait"].append(wait_end - wait_start)
                    process_batch(batch, wait_end, metadata)

            worker_thread = threading.Thread(target=processing_worker)
            worker_thread.start()

            recv_start = time.time()
            for chunk in reader:
                recv_end = time.time()
                cost_break_down["receiving"].append(recv_end - recv_start)
                q.put((chunk.data, chunk.app_metadata))
                recv_start = time.time()

            q.put(None)
            worker_thread.join()
        else:
            recv_start = time.time()
            for chunk in reader:
                recv_end = time.time()
                cost_break_down["receiving"].append(recv_end - recv_start)
                process_batch(chunk.data, recv_end, chunk.app_metadata)
                recv_start = time.time()

        if windowing:
            with self.windowing_lock:
                self.active_sources[source_key]["is_finished"] = True

                to_flush = self._collect_windows_to_flush(window_size, window_slide)

                result = []
                for window_end, window_slides in to_flush:
                    batches = self._materialize_window_batches(
                        window_end, window_slides, window_slide
                    )
                    if not batches:
                        continue
                    ctx.register_record_batches(self.default_table_name, [batches])
                    result_df = ctx.sql(query)
                    result.extend(result_df.collect())
                    ctx.deregister_table(self.default_table_name)

            for j, result_batch in enumerate(result):
                # output_rows += result_batch.num_rows
                if use_dictionary_encoding:
                    assert cfg.columns_to_encode is not None
                    result_batch = dictionary_encode_batch(
                        result_batch, cfg.columns_to_encode
                    )

                forward_writer.write_batch(result_batch)

        forward_writer.done_writing()
        end = time.time()

        duration = end - start
        gbps = (output_bytes * 8) / (duration * 1000**3)
        mbps = output_bytes / (duration * 1000**2)
        filtered_ratio = 1 - (output_bytes / input_bytes) if input_bytes > 0 else 0
        filtered_ratio_rows = 1 - (output_rows / input_rows) if input_rows > 0 else 0

        log = {
            "experiment_id": experiment_id,
            "iteration_id": iteration_id,
            "source_node_id": source_node_id,
            "thread_id": thread_id,
            "input_bytes": input_bytes,
            "output_bytes": output_bytes,
            "input_rows": input_rows,
            "output_rows": output_rows,
            "filtered_ratio": round(filtered_ratio, 4),
            "filtered_ratio_rows": round(filtered_ratio_rows, 4),
            "start_time": start,
            "duration": duration,
            "MBps": f"{mbps:.2f}",
            "Gbps": f"{gbps:.4f}",
            "receiving": sum(cost_break_down["receiving"]),
            "decoding": sum(cost_break_down["decoding"]),
            "querying": sum(cost_break_down["querying"]),
            "encoding": sum(cost_break_down["encoding"]),
            "sending": sum(cost_break_down["sending"]),
            "queue_wait": sum(cost_break_down["queue_wait"]),
            "forward_times": forwarding_times,
        }
        with self.logs_lock:
            self.logs.append(log)
        log_str = json.dumps(log)
        print(log_str)

        with self.open_requests_lock:
            self.open_requests -= 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Processing Node")
    parser.add_argument(
        "--port",
        type=int,
        default=8010,
        help="Port to run the WoolMilk processing node",
    )
    parser.add_argument(
        "--forward-node",
        type=str,
        default="localhost:8020",
        help="Address of the node to forward data to (host:port)",
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="SQL query to run on incoming batches",
    )
    parser.add_argument(
        "--query-result-schema",
        type=str,
        default=None,
        help="JSON schema definition for the query result",
    )
    parser.add_argument("--window-size", type=int, default=None)
    parser.add_argument("--window-slide", type=int, default=None)
    parser.add_argument(
        "--tuples-per-batch",
        type=int,
        default=8192,
        help="DataFusion SessionContext batch size",
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
        help="Encoding applied to string columns on outbound Flight payload",
    )
    parser.add_argument(
        "--columns-to-encode",
        type=str,
        default=None,
        help="Comma-separated columns to dictionary-encode (e.g. city,name)",
    )
    parser.add_argument(
        "--use-buffering",
        action="store_true",
        help="Enable buffering on processing node",
    )
    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Processing Node Parameters")
    print("=" * 40)
    print(f" Port               : {args.port}")
    print(f" Forward Node       : {args.forward_node}")
    print(f" SQL Query          : {args.query}")
    print(f" Schema             : {args.query_result_schema}")
    print(f" Window Size        : {args.window_size}")
    print(f" Window Slide       : {args.window_slide}")
    print(f" Compression        : {args.compression}")
    print(f" Encoding           : {args.encoding}")
    print(f" Columns to encode  : {args.columns_to_encode}")
    print(f" Use Buffering      : {args.use_buffering}")
    print(f" Tuples/Batch       : {args.tuples_per_batch}")
    print("=" * 40 + "\n")

    schema_dict = (
        json.loads(args.query_result_schema) if args.query_result_schema else None
    )
    columns_to_encode = (
        args.columns_to_encode.split(",") if args.columns_to_encode else None
    )
    windowing = (
        {"window_size": args.window_size, "window_slide": args.window_slide}
        if args.window_size and args.window_slide
        else None
    )
    startup_cfg = RuntimeConfig(
        compression=args.compression,
        encoding=args.encoding,
        columns_to_encode=columns_to_encode,
        use_buffering=args.use_buffering,
        tuples_per_batch=args.tuples_per_batch,
        query=args.query,
        query_result_schema=schema_dict,
        windowing=windowing,
    )
    processing_node = ProcessingNode(
        location=f"grpc://0.0.0.0:{args.port}",
        forward_node=args.forward_node,
        runtime_config=startup_cfg,
    )
    print(f"WoolMilk processing node running on port {args.port}")
    processing_node.serve()
