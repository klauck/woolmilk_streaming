"""
    ---- Sink Node Implementation ----
    Collects Data from Processing Node (via PUSH)
"""

import argparse
import json
import os
import threading
import time

import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet as pq
from pyarrow import flight

from tools.metrics import str2bool, LogType, MonitorService, MetricConfig, LIMIT, LogObject, UNDEFINED_MONITOR, \
    HealthResult


class SinkNode(pa.flight.FlightServerBase):
    def __init__(self, location, advertised_host: str, result_folder=None, monitor: MonitorService = None):
        super().__init__(location)
        self.result_folder = result_folder
        if result_folder:
            os.makedirs(result_folder, exist_ok=True)
        self.file_counter = 0
        self.file_counter_lock = threading.Lock()
        self.monitor = monitor

        if self.monitor:
            self.monitor.connect_monitor(advertised_host)

    def do_action(self, context, action):
        t = action.type

        if t == "health":
            if self.monitor is None:
                raise flight.FlightServerError(UNDEFINED_MONITOR)
            with self.monitor.lock:
                payload: HealthResult = self.monitor.check_health()
                yield flight.Result(json.dumps(payload.to_dict()).encode("utf-8"))

        elif t == "logs" or t == "all_logs":
            #TODO
            if self.monitor is None:
                raise flight.FlightServerError(UNDEFINED_MONITOR)
            with self.monitor.lock:
                logs: list[LogObject] = self.monitor.parse_logs(t == "all_logs")
                payload: list[dict[str, str]] = [log.to_dict() for log in logs]
                yield flight.Result(json.dumps(payload).encode("utf-8"))

        elif t == "shutdown":
            #TODO
            if self.monitor is None:
                raise flight.FlightUnauthorizedError(UNDEFINED_MONITOR)

            def shutdown_server():
                time.sleep(0.1)
                self.shutdown()

            threading.Thread(target=shutdown_server, daemon=True).start()
            yield flight.Result(b"Server shutting down")
        else:
            if self.monitor:
                self.monitor.logs.append(LogObject(
                    f"SinkNode::do_action::Unknown action call '{action.type}'",
                    LogType.WARN))
            raise flight.FlightServerError(f"Unknown action: '{action.type}'")

    def do_put(self, context, descriptor, reader, writer):
        try:
            self.batch_handler(reader)

        except Exception as e:
            if self.monitor:
                self.monitor.logs.append(LogObject(
                    f"SinkNode::do_put::{e}",
                    LogType.ERROR))
                self.monitor._internal_error = True

            raise flight.FlightServerError(f"SinkNode do_put failed: {e}") from e

    def batch_handler(self, reader):
        total_bytes = 0
        receive_times = []
        start = receive_start = time.time()
        result = []
        for chunk in reader:
            batch = chunk.data
            # execute and forward data here
            total_bytes += batch.nbytes
            if self.result_folder:
                result.append(batch)
            receive_end = time.time()
            receive_times.append((receive_start, receive_end))
            receive_start = receive_end
        end = time.time()

        duration = end - start
        gbps = (total_bytes * 8) / (duration * 1000**3)
        mbps = total_bytes / (duration * 1000**2)

        print(
            f'WM_LOG= {{"received_bytes": {total_bytes}, "start_time": {start},'
            f' "duration": {duration}, "MBps": {mbps:.2f}, "Gbps": {gbps:.4f}}}'
        )
        print(f"End: {end}")
        print("receive_times = ", receive_times)
        with self.file_counter_lock:
            local_id = self.file_counter
            self.file_counter += 1

        if self.result_folder:
            if result:
                # Result is not empty
                table = pa.Table.from_batches(result)
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
        "--monitor", type=str2bool, default=True, help="Allow Monitor?"
    )

    parser.add_argument(
        "--monitor_url", type=str, default="localhost:8000", help="Monitor URL"
    )

    parser.add_argument(
        "--advertised-host", type=str, default="localhost", help="URL reachable from Outside"
    )

    parser.add_argument(
        "--metric_cpu_warn", type=float, default=70.0, help="CPU warning metric"
    )
    parser.add_argument(
        "--metric_mem_warn", type=float, default=70.0, help="Memory warning metric"
    )

    parser.add_argument(
        "--metric_cpu_critical", type=float, default=85.0, help="CPU critical metric"
    )

    parser.add_argument(
        "--metric_mem_critical", type=float, default=80.0, help="Memory critical metric"
    )

    parser.add_argument(
        "--log-level", type=LogType, default=LogType.WARN, help="Log level"
    )

    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Sink Node Parameters")
    print("=" * 40)
    print(f" Port        : {args.port}")
    if args.monitor:
        print(f" Monitor URL : grpc://{args.monitor_url}")
    print("=" * 40 + "\n")

    location = f"grpc://0.0.0.0:{args.port}"
    result_folder = args.result_folder
    if args.result_folder == "None":
        result_folder = None

    monitorService: MonitorService | None = None
    if args.monitor:
        monitorService = MonitorService(MetricConfig(LIMIT(args.metric_cpu_warn, args.metric_cpu_critical),
                                                     LIMIT(args.metric_mem_warn, args.metric_mem_critical)),
                                        f"grpc://{args.monitor_url}")

    sink_node = SinkNode(location, f"{args.advertised_host}:{args.port}", result_folder, monitorService)
    print(f"WoolMilk sink node running at {location}")
    try:
        sink_node.serve()
    except KeyboardInterrupt:
        pass
    if args.monitor and sink_node.monitor.monitor_connected:
        sink_node.monitor.disconnect_monitor(f"{args.advertised_host}:{args.port}")
    print("Shutting down Sink Node.")
    (sink_node
     .shutdown())
    exit(0)
