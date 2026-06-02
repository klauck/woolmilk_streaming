import argparse
import json
import queue
import threading
import time

import pyarrow as pa
import pyarrow.flight
from datafusion import SessionConfig, SessionContext

from woolmilk.encoding import (
    dictionary_decode_batch,
    dictionary_encode_batch,
    dictionary_encode_schema,
    get_compressed_flight_options,
)
from woolmilk.source_node import NodeStatus, SourceNodeActions

DEFAULT_DATAFUSION_BATCH_SIZE = 8192


class ProcessingNode(pa.flight.FlightServerBase):
    def __init__(self, location, forward_node, sql_query, schema_json):
        super().__init__(location)
        self.forwarding_client = pa.flight.FlightClient(f"grpc://{forward_node}")
        self.query = sql_query
        self.default_table_name = "nexmark_data"
        self.logs = []
        self.logs_lock = threading.Lock()
        self.open_requests = 0
        self.open_requests_lock = threading.Lock()

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

    def do_action(self, context, action):
        if action.type == "get_logs":
            with self.logs_lock:
                logs = self.logs
            yield pyarrow.flight.Result(json.dumps(logs).encode("utf-8"))
        elif action.type == "delete_logs":
            with self.logs_lock:
                self.logs = []
        elif action.type == SourceNodeActions.GET_STATUS:
            with self.open_requests_lock:
                if self.open_requests == 0:
                    status = NodeStatus.IDLE
                else:
                    status = NodeStatus.SENDING_DATA
            yield pyarrow.flight.Result(status.encode("utf-8"))
        else:
            raise NotImplementedError(f"Unknown action: {action.type}")

    def do_put(self, context, descriptor, reader, writer):
        with self.open_requests_lock:
            self.open_requests += 1

        # data for path info
        experiment_id = None
        iteration_id = None
        source_node_id = None
        thread_id = None

        use_dictionary_encoding = False
        use_compression = False
        use_buffering = False  # Dynamic fallback
        tuples_per_batch = DEFAULT_DATAFUSION_BATCH_SIZE
        effective_query = self.query

        incoming_path_info = {}

        try:
            incoming_path_info = json.loads(descriptor.path[0].decode("utf-8"))
            if isinstance(incoming_path_info, dict):
                experiment_id = incoming_path_info.get("experiment_id")
                iteration_id = incoming_path_info.get("iteration_id")
                source_node_id = incoming_path_info.get("source_node_id")
                thread_id = incoming_path_info.get("thread_id")
                use_dictionary_encoding = incoming_path_info.get(
                    "use_dictionary_encoding", False
                )
                use_compression = incoming_path_info.get("use_compression", False)
                use_buffering = incoming_path_info.get("use_buffering", False)
                tuples_per_batch = incoming_path_info.get(
                    "tuples_per_batch", DEFAULT_DATAFUSION_BATCH_SIZE
                )
                path_query = incoming_path_info.get("query")
                if path_query:
                    effective_query = path_query

        except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
            pass

        ctx = SessionContext(SessionConfig().with_batch_size(tuples_per_batch))

        # we forward same path information to the next node
        forwarded_path_info = json.dumps(incoming_path_info)

        target_schema = self.predefined_schema
        if use_dictionary_encoding:
            target_schema = dictionary_encode_schema(target_schema)

        call_options = None
        if use_compression:
            call_options = get_compressed_flight_options()

        forward_writer, _ = self.forwarding_client.do_put(
            pa.flight.FlightDescriptor.for_path(forwarded_path_info),
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

        def decode_metadata(meta):
            if meta is None:
                return None
            try:
                return bytes(meta).decode("utf-8")
            except Exception:
                return None

        def process_batch(batch, batch_in_hand_t, incoming_metadata):
            nonlocal input_bytes, output_bytes, input_rows, output_rows

            batch_id = decode_metadata(incoming_metadata)

            decoding_start = time.time()
            if use_dictionary_encoding:
                batch = dictionary_decode_batch(batch)
            decoding_end = time.time()

            input_bytes += batch.nbytes
            input_rows += batch.num_rows

            querying_start = time.time()
            ctx.register_record_batches(self.default_table_name, [[batch]])
            result_df = ctx.sql(effective_query)
            result = result_df.collect()
            ctx.deregister_table(self.default_table_name)
            querying_end = time.time()

            encoding_total = 0.0
            sending_total = 0.0
            batch_output_bytes = 0
            for j, result_batch in enumerate(result):
                output_rows += result_batch.num_rows
                if use_dictionary_encoding:
                    enc_start = time.time()
                    result_batch = dictionary_encode_batch(result_batch)
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

        if use_buffering:
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
        default="SELECT * FROM nexmark_data",
        help="SQL query to run on incoming batches",
    )
    parser.add_argument(
        "--query-result-schema",
        type=str,
        required=True,
        help="JSON schema definition for the data (required)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Processing Node Parameters")
    print("=" * 40)
    print(f" Port           : {args.port}")
    print(f" Forward Node   : {args.forward_node}")
    print(f" SQL Query      : {args.query}")
    print(f" Schema         : {args.query_result_schema}")
    print("=" * 40 + "\n")

    port = args.port
    forward_node = args.forward_node
    sql_query = args.query
    schema_json = args.query_result_schema
    processing_node = ProcessingNode(
        f"grpc://0.0.0.0:{port}",
        forward_node,
        sql_query,
        schema_json,
    )
    print(f"WoolMilk processing node running on port {port}")
    processing_node.serve()
