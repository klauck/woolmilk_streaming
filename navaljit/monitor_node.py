"""
    --- Woolmilk Monitor Node ---
    Monitors all Nodes (Sink, Processing and Sources) and provides REST for UI
"""

import argparse
import json
import threading
import time
from collections import deque

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from pyarrow import flight as pf
import pyarrow as pa
import pyarrow.compute as pc

from tools.metrics import METRIC_SCHEMA, HealthResult, HealthStatus
from tools.logger import LogService, LogType
from tools.monitor import NodeInfo, NodeType

Logger: LogService | None = None

MAX_ITEMS_DEQUE_SIZE = 10


class NotFoundException(Exception):
    def __init__(self):
        super().__init__()


# ==========================
# APACHE ARROW FLIGHTSERVER
# ==========================

class MonitorNode(pf.FlightServerBase):
    def __init__(self, location, poll_interval: int, max_window: int):
        super().__init__(location)
        self.nodes: dict[str, NodeInfo] = {}
        self.lock = threading.Lock()
        self.poll_interval = poll_interval  # seconds
        self.window_size = max_window
        self.agg_metrics: dict[str, deque[pa.RecordBatch]] = {}
        self.poll_thread = threading.Thread(target=self.poll_metrics, args=(), daemon=True)
        self.poll_thread.start()

    def shutdown(self):
        self.poll_thread.join(timeout=3)
        super().shutdown()

    def do_action(self, context, action):
        if action.type == "register":
            payload = action.body.to_pybytes().decode("utf-8")
            data = json.loads(payload)

            node_url: str = data.get("url")
            forward_urls: list[str] = data.get("forward_urls", [])
            node_type: NodeType = data.get("type")
            query: str = data.get("query", "")

            with self.lock:
                if node_url not in self.nodes:
                    Logger.log(f"Registering new client for {node_url}", LogType.INFO)
                    self.nodes[node_url] = NodeInfo(node_url, node_type, forward_urls, query)
                    self.agg_metrics[node_url] = deque(maxlen=self.window_size)
                else:
                    node = self.nodes[node_url]
                    node.forward_urls = forward_urls
                    node.query = query
                    Logger.log(f"Client {node_url} has reconnected.", LogType.INFO)

            yield pf.Result(b"OK")

        elif action.type == "disconnect":
            node_url = action.body.to_pybytes().decode("utf-8")
            with self.lock:
                if node_url in self.nodes:
                    del self.nodes[node_url]
                    self.agg_metrics[node_url].clear()
                    del self.agg_metrics[node_url]
                    Logger.log(f"Disconnected node {node_url}", LogType.INFO)
            yield pf.Result(b"Disconnected")

    def poll_logs(self):
        while True:
            with self.lock:
                nodes_copy = list(self.nodes.items())

            for url, node_info in nodes_copy:
                try:
                    client: pf.FlightClient = node_info.client
                    res = next(client.do_action(pf.Action("logs", b"")), None)
                    if res is None: continue
                    payload = json.loads(res.body.to_pybytes().decode("utf-8"))
                    Logger.log(f"Poll Logs: {url}: {payload}", LogType.DEBUG)

                except Exception as e:
                    Logger.log(f"[MonitorNode] Failed to poll logs from {url}: {e}", LogType.ERROR)

            time.sleep(self.poll_interval)

    def poll_metrics(self):
        while True:
            with self.lock:
                nodes_copy = list(self.nodes.items())

            for url, node_info in nodes_copy:
                try:
                    client: pf.FlightClient = node_info.client
                    res = next(client.do_action(pf.Action("metrics", b"")), None)
                    if res is None: continue
                    buf = res.body.to_pybytes()
                    if not buf: continue

                    batch = pa.ipc.read_record_batch(pa.BufferReader(buf), METRIC_SCHEMA)
                    self.aggregate_metrics(batch, url)

                except StopIteration:
                    continue

                except Exception as e:
                    print(f"[MonitorNode] Failed to poll metrics from {url}: {e}")

            time.sleep(self.poll_interval)

    def api_get_metrics(self, node_url: str):
        with self.lock:
            metrics = self.agg_metrics.get(node_url)
            print(metrics is not None)
            if metrics is None:
                return None
            batches = list(metrics)

        if len(batches) == 0:
            return {}
        table = pa.Table.from_batches(batches)
        data = table.to_pylist()
        return data

    def check_health_client(self, client: pf.FlightClient) -> HealthResult:
        try:
            raw = client.do_action(pf.Action("health", b""))
            res = next(raw, None)

            if res is None:
                return HealthResult(HealthStatus.UNKNOWN, 0, 0)

            status = json.loads(res.body.to_pybytes().decode("utf-8"))
            return HealthResult(HealthStatus(status["STATUS"]), status["CPU"], status["MEM"])

        except (pf.FlightUnavailableError, pf.FlightTimedOutError, pf.FlightCancelledError):
            return HealthResult(HealthStatus.UNKNOWN, 0, 0)

        except Exception as e:
            print(f"Health check failed: {e}")
            return HealthResult(HealthStatus.UNKNOWN, 0, 0)

    def api_get_all_health(self):
        result: dict[str, dict] = {}

        for url, node in self.nodes.items():
            health = self.check_health_client(node.client)
            result[url] = health.to_dict()

        return result


    def api_get_health(self, url: str):
        if self.nodes.get(url) is None:
            return None
        return self.check_health_client(self.nodes[url].client).to_dict()

    def aggregate_metrics(self, batch: pa.RecordBatch, host_url: str):
        t: pa.Table = pa.Table.from_batches([batch])
        grouped: pa.Table = t.group_by(["address", "type"]).aggregate([
            ("duration_ns", "sum"),
            ("duration_ns", "mean"),
            ("bytes", "sum"),
            ("bytes", "mean"),
            ("duration_ns", "count"),
        ])

        if grouped.num_rows == 0:
            empty_batch = pa.RecordBatch.from_arrays(
                [
                    pa.array([], type=pa.timestamp("s")),
                    pa.array([], type=pa.string()),
                    pa.array([], type=pa.string()),
                    pa.array([], type=pa.int64()),
                    pa.array([], type=pa.float64()),
                    pa.array([], type=pa.int64()),
                    pa.array([], type=pa.float64()),
                    pa.array([], type=pa.int64()),
                    pa.array([], type=pa.float64()),
                ],
                names=[
                    "timestamp",
                    "address",
                    "type",
                    "total_duration_ns",
                    "avg_duration_ns",
                    "total_bytes",
                    "avg_bytes",
                    "count",
                    "avg_bandwidth",
                ]
            )
            self.agg_metrics[host_url].append(empty_batch)
            return

        bytes_sum_f = pc.cast(grouped["bytes_sum"], "float64")
        dur_sum_f = pc.cast(grouped["duration_ns_sum"], "float64")

        raw_bw = pc.multiply(pc.divide(bytes_sum_f, dur_sum_f), 1000.0)
        avg_bandwidth = pc.if_else(
            pc.equal(grouped["duration_ns_sum"], 0),
            pa.scalar(0.0),
            raw_bw
        )
        avg_bandwidth = pc.round(avg_bandwidth, 2)

        ts_array = pa.array([time.time()] * grouped.num_rows, type=pa.timestamp("s"))

        result: pa.Table = (grouped.add_column(0, "timestamp", ts_array).append_column("avg_bandwidth", avg_bandwidth)
                            .rename_columns(
            ["timestamp", "address", "type", "total_duration_ns", "avg_duration_ns", "total_bytes", "avg_bytes",
             "count", "avg_bandwidth"]))

        self.agg_metrics[host_url].append(result.to_batches()[0])


# ======================
# REST-API mit FastAPI
# ======================

monitor: MonitorNode = None
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/nodes")
def get_all_nodes():
    return [
        {
            "url": url,
            "type": node.node_type,
            "forward_urls": node.forward_urls,
            "query": node.query,
        }
        for url, node in monitor.nodes.items()
    ]


@app.get("/nodes/health")
def get_all_health():
    return monitor.api_get_all_health()


@app.get("/nodes/{node}")
def get_node(node: str):
    if node not in monitor.nodes:
        raise HTTPException(status_code=404, detail=f"Node {node} not found")
    else:
        return {
            "url": node,
            "type": monitor.nodes[node].node_type,
            "forward_urls": monitor.nodes[node].forward_urls,
            "query": monitor.nodes[node].query,
        }


@app.get("/nodes/{node}/health")
def get_all_health(node: str):
    result = monitor.api_get_health(node)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Node {node} not found")
    else:
        return result


@app.get("/nodes/{node}/metrics")
def get_metrics(node: str):
    result = monitor.api_get_metrics(node)

    if result is None:
        raise HTTPException(status_code=404, detail=f"Node {node} not found")
    else:
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Monitor Node")
    parser.add_argument(
        "--flight-port",
        type=int,
        default=7999,
        help="Port to run the WoolMilk Monitor Node",
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=7998,
        help="Port to run the WoolMilk Monitor REST",
    )

    parser.add_argument(
        "--poll_interval",
        type=int,
        default=5,
        help="Poll Interval in seconds",
    )

    parser.add_argument(
        "--max-window",
        type=int,
        default=12,
        help="Queue contains last x Polls",
    )

    parser.add_argument(
        "--log-save-level", type=LogType, default=LogType.INFO, help="Log level"
    )
    parser.add_argument(
        "--log-print-level", type=LogType, default=LogType.DEBUG, help="Log level"
    )

    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Monitor Configuration")
    print("=" * 40)
    print(f" Flight gRPC Server : grpc://0.0.0.0:{args.flight_port}")
    print(f" REST API           : http://0.0.0.0:{args.api_port}")
    print("=" * 40 + "\n")

    Logger = LogService(args.log_save_level, args.log_print_level)
    Logger.log("Starting WoolMilk Sink Node", LogType.INFO)

    monitor = MonitorNode(f"grpc://0.0.0.0:{args.flight_port}", args.poll_interval, args.max_window)

    try:
        flight_thread = threading.Thread(target=lambda: monitor.serve, daemon=True)
        flight_thread.start()
        uvicorn.run(app, host="0.0.0.0", port=args.api_port)

    except KeyboardInterrupt:
        Logger.log("Shutting down WoolMilk Monitor Node", LogType.INFO)
        monitor.shutdown()
        flight_thread.join()
        exit(0)
