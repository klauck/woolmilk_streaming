import argparse
import json
import os
import threading
import time

import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet as pq
from datafusion import SessionContext


class SinkNode(pa.flight.FlightServerBase):
    def __init__(
        self,
        location,
        mode,
        merge_query,
        expected_senders,
        result_folder=None
    ):
        super().__init__(location)
        self.result_folder = result_folder
        if result_folder:
            os.makedirs(result_folder, exist_ok=True)

        self.mode = mode
        self.merge_query = merge_query
        self.expected_senders = expected_senders

        self.file_counter = 0
        self.file_counter_lock = threading.Lock()

        self.lock = threading.Lock()

        # For each window, store all batches belonging to it
        self.window_batches = {}
        # For each window, store which senders already contributed their batch to it
        self.window_contributors = {}
        # Track finished senders to avoid dead windows
        self.finished_senders = {}
        # Counts connections established via do_put
        self.sender_counter = 0


    def _collect_windows_to_flush(self):
        if not self.result_folder:
            return

        windows_to_flush = []

        with self.lock:
            # Don't flush until all senders connected
            if len(self.finished_senders) < self.expected_senders:
                return

            # Prepare to flush window if all senders either contributed or are finished
            for window_key, contributors in self.window_contributors.items():
                finished_or_contributed = 0
                for sender_id, is_finished in self.finished_senders.items():
                    if is_finished or sender_id in contributors:
                        finished_or_contributed += 1

                if finished_or_contributed >= self.expected_senders:
                    windows_to_flush.append(window_key)

        # Flush ready windows (outside the lock)
        for window_key in sorted(windows_to_flush):
            self._flush_window(window_key)


    def _flush_window(self, window_key: tuple[int, int]):
        window_start, window_end = window_key

        with self.lock:
            batches = self.window_batches.pop(window_key, [])
            self.window_contributors.pop(window_key, None)

        if not batches:
            return

        ctx = SessionContext()
        ctx.register_record_batches("partial_results", partitions=[batches])

        df = ctx.sql(self.merge_query)
        merged_batches = df.collect()

        ctx.deregister_table("partial_results")

        if self.result_folder and merged_batches:
            table = pa.Table.from_batches(merged_batches)
            out_path = f"{self.result_folder}/window_{window_start}_{window_end}.parquet"
            pq.write_table(table, out_path)
            print(f"Wrote unified window result -> {out_path}")


    def do_put(self, context, descriptor, reader, writer):
        total_bytes = 0
        receive_times = []
        start = receive_start = time.time()

        with self.lock:
            sender_id = self.sender_counter
            self.sender_counter += 1
            self.finished_senders.setdefault(sender_id, False)

        passthrough_result = []

        for chunk in reader:
            batch = chunk.data
            total_bytes += batch.nbytes

            if self.mode == "windowed":
                if chunk.app_metadata is None:
                    raise ValueError("Metadata missing")
                raw = chunk.app_metadata.to_pybytes()
                meta = json.loads(raw.decode("utf-8"))
                window_key = (meta["window_start"], meta["window_end"])

                with self.lock:
                    self.window_batches.setdefault(window_key, []).append(batch)
                    self.window_contributors.setdefault(window_key, set()).add(sender_id)

                self._collect_windows_to_flush()

            else:
                if self.result_folder:
                    passthrough_result.append(batch)


            receive_end = time.time()
            receive_times.append((receive_start, receive_end))
            receive_start = receive_end

        if self.mode == "windowed":
            with self.lock:
                self.finished_senders[sender_id] = True
            self._collect_windows_to_flush()

        end = time.time()
        duration = end - start
        gbps = (total_bytes * 8) / (duration * 1000**3) if duration > 0 else 0.0
        mbps = total_bytes / (duration * 1000**2) if duration > 0 else 0.0

        print(
            f'WM_LOG= {{"received_bytes": {total_bytes}, "start_time": {start},'
            f' "duration": {duration}, "MBps": {mbps:.2f}, "Gbps": {gbps:.4f}}}'
        )
        print(f"End: {end}")
        print("receive_times = ", receive_times)

        if self.mode == "default":
            with self.file_counter_lock:
                local_id = self.file_counter
                self.file_counter += 1

            if self.result_folder and passthrough_result:
                table = pa.Table.from_batches(passthrough_result)
                pq.write_table(table, f"{self.result_folder}/{local_id}.parquet")
                print(f"Wrote .. {self.result_folder}/{local_id}.parquet")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Sink Node")
    parser.add_argument(
        "--port", type=int, default=8020, help="Port to run the WoolMilk sink node"
    )
    parser.add_argument(
        "--result-folder", type=str, default="None", help="Folder to store results"
    )
    parser.add_argument(
        "--mode",
        choices=["default", "windowed"],
        default="default",
        help="default: write batches as received, windowed: unify partial window results via --merge-query",
    )
    parser.add_argument(
        "--merge-query",
        type=str,
        default=None,
        help="SQL query to unify partial window results. Used only in --mode windowed. "
             "Table name is 'partial_results'.",
    )
    parser.add_argument(
        "--expected-senders",
        type=int,
        default=1,
        help="How many processing nodes are expected to send results to this sink (required in windowed mode).",
    )
    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Sink Node Parameters")
    print("=" * 40)
    print(f" Port             : {args.port}")
    print(f" Mode             : {args.mode}")
    print(f" Expected Senders : {args.expected_senders}")
    print("=" * 40 + "\n")

    location = f"grpc://0.0.0.0:{args.port}"
    result_folder = args.result_folder
    if args.result_folder == "None":
        result_folder = None

    if args.mode == "windowed":
        if not args.merge_query:
            raise ValueError("--mode windowed requires --merge-query")
        if args.expected_senders <= 0:
            raise ValueError("--mode windowed requires --expected-senders > 0")

    sink_node = SinkNode(
        location,
        result_folder,
        mode=args.mode,
        merge_query=args.merge_query,
        expected_senders=args.expected_senders,
    )
    print(f"WoolMilk sink node running at {location}")
    sink_node.serve()