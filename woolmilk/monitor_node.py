"""
    --- Woolmilk Monitor Node ---
    Monitors all Nodes (Sink, Processing and Sources) and provides REST for UI
"""

import argparse
import json
import threading
import time

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from pyarrow import flight

from tools.metrics import (HealthResult, HealthStatus, NodeInfo, RawProcessingMetric,
                           RawTransferMetric, AggregatedProcessingMetric, AggregatedTransferMetric)


# ==========================
# APACHE ARROW FLIGHTSERVER
# ==========================

class MonitorNode(flight.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)
        self.nodes: dict[str, NodeInfo] = {}
        self.lock = threading.Lock()

        # Dictionary(NODE_ID, (SOURCE, (TIMESTAMP, VALUE)))
        self.aggregated_processing_metrics: dict[str, dict[str, dict[int, AggregatedProcessingMetric]]] = {}
        # Dictionary(NODE_ID, (SOURCE, (SINK, (TIMESTAMP, VALUE))))
        self.aggregated_transfer_metrics: dict[str, dict[str, dict[str, dict[int, AggregatedTransferMetric]]]] = {}
        self.poll_interval = 1   #1s
        self.window_size = 1
        self.keep_window_size = 300 #5min max Einträge

        threading.Thread(target=self.poll_metrics, daemon=True).start()

    def poll_metrics(self):
        while True:
            with self.lock:
                nodes_copy = list(self.nodes.items())

            ### Poll metrics from all nodes
            for node_url, node_info in nodes_copy:
                try:
                    raw = node_info.client.do_action(flight.Action("metrics", b""))
                    res = next(raw, None)

                    if res is None: continue

                    payload = json.loads(res.body.to_pybytes().decode("utf-8"))
                    #print(f"Poll Metrics: {node_url}, {payload}")

                    raw_processing = [
                        RawProcessingMetric.from_dict(d) for d in payload.get("processing_metrics", [])
                    ]
                    raw_transfer = [
                        RawTransferMetric.from_dict(d) for d in payload.get("transfer_metrics", [])
                    ]
                    self.aggregate_metrics(node_url, raw_processing, raw_transfer)

                except Exception as e:
                    print(f"[MonitorNode] Failed to poll metrics from {node_url}: {e}")

            time.sleep(self.poll_interval)

    def aggregate_metrics(self, node_id: str, raw_processing: list[RawProcessingMetric], raw_transfer: list[RawTransferMetric]):
        ### Aggregation for processing ####
        if node_id not in self.aggregated_processing_metrics:
            self.aggregated_processing_metrics[node_id] = {}

        for m in raw_processing:
            source = m.source
            window: int = int(m.timestamp // self.window_size) * self.window_size


            if source not in self.aggregated_processing_metrics[node_id]:
                self.aggregated_processing_metrics[node_id][source] = {}

            if window not in self.aggregated_processing_metrics[node_id][source]:
                self.aggregated_processing_metrics[node_id][source][window] = AggregatedProcessingMetric(source, 0, 0, window,0)

            current_metric: AggregatedProcessingMetric = self.aggregated_processing_metrics[node_id][source][window]
            current_metric.total_size += m.size
            current_metric.count += 1
            current_metric.total_duration += m.duration
            print(current_metric)


        ### Aggregation for transfer ###
        if node_id not in self.aggregated_transfer_metrics:
            self.aggregated_transfer_metrics[node_id] = {}

        for m in raw_transfer:
            source = m.source
            client = m.client
            window: int = int(m.timestamp // self.window_size) * self.window_size

            if source not in self.aggregated_transfer_metrics[node_id]:
                self.aggregated_transfer_metrics[node_id][source] = {}

            if client not in self.aggregated_transfer_metrics[node_id][source]:
                self.aggregated_transfer_metrics[node_id][source][client] = {}

            if window not in self.aggregated_transfer_metrics[node_id][source][client]:
                self.aggregated_transfer_metrics[node_id][source][client][window] = AggregatedTransferMetric(source, client, 0, window, 0, 0)

            current_metric: AggregatedTransferMetric = self.aggregated_transfer_metrics[node_id][source][client][window]
            current_metric.total_size += m.size
            current_metric.count += 1
            current_metric.total_duration += m.duration


        self.clean_aggregated_metric()


    def clean_aggregated_metric(self):
        now = int(time.time())
        cutoff = now - self.keep_window_size

        # PROCESSING
        empty_nodes = []
        for node_id, per_source in self.aggregated_processing_metrics.items():

            empty_sources = []
            for source, windows in per_source.items():
                new_windows = {ts: m for ts, m in windows.items() if ts >= cutoff}
                if len(new_windows) == 0:
                    empty_sources.append(source)
                else:
                    per_source[source] = new_windows

            for s in empty_sources:
                del per_source[s]

        for node in empty_nodes:
            del self.aggregated_processing_metrics[node]



        # TRANSFERING
        empty_nodes = []
        for node_id, per_source in self.aggregated_transfer_metrics.items():
            empty_sources = []

            for source, per_client in per_source.items():
                empty_clients = []

                for client, windows in per_client.items():
                    new_windows = {ts: m for ts, m in windows.items() if ts >= cutoff}

                    if len(new_windows) == 0:
                        empty_clients.append(client)
                    else:
                        per_client[client] = new_windows

                for client in empty_clients:
                    del per_client[client]

                if len(per_client) == 0:
                    empty_sources.append(source)

            for s in empty_sources:
                del per_source[s]

            if len(per_source) == 0:
                empty_nodes.append(node_id)

        for node in empty_nodes:
            del self.aggregated_transfer_metrics[node]


    def do_action(self, context, action):
        if action.type == "register":
            payload = action.body.to_pybytes().decode("utf-8")
            data = json.loads(payload)

            node_url: str = data.get("URL")
            forward_urls: list[str] = data.get("ForwardURLS", [])
            query: str = data.get("QUERY", "")

            with self.lock:
                if node_url not in self.nodes:
                    print(f"Registering new client for {node_url}")
                    self.nodes[node_url] = NodeInfo(node_url, forward_urls, query)
                else:
                    node = self.nodes[node_url]
                    node.forward_urls = forward_urls
                    node.query = query
                    print(f"Client {node_url} has reconnected.")

            yield flight.Result(b"OK")

        elif action.type == "disconnect":
            node_url = action.body.to_pybytes().decode("utf-8")
            with self.lock:
                if node_url in self.nodes:
                    del self.nodes[node_url]
                    print(f"Disconnected node {node_url}")
            yield flight.Result(b"Disconnected")

    def check_health_client(self, client: flight.FlightClient) ->  HealthResult:
        try:
            raw = client.do_action(flight.Action("health", b""))
            res = next(raw, None)

            if res is None:
                return HealthResult(HealthStatus.DOWN, 0, 0)

            status = json.loads(res.body.to_pybytes().decode("utf-8"))
            return HealthResult(HealthStatus(status["STATUS"]), status["CPU"], status["MEM"])

        except (flight.FlightUnavailableError, flight.FlightTimedOutError, flight.FlightCancelledError):
            return HealthResult(HealthStatus.DOWN, 0, 0)

        except Exception as e:
            print(f"Health check failed: {e}")
            return HealthResult(HealthStatus.DOWN, 0, 0)

    def get_nodes_info(self) -> list[dict]:
        result: list[dict] = []

        for url, node in self.nodes.items():
            health = self.check_health_client(node.client)
            result.append({"URL": url, "STATUS": health.to_dict(), "FORWARD_URLS": node.forward_urls, "QUERY": node.query})

        return result

    def get_nodes_metrics(self, url: str):
        processing_raw = self.aggregated_processing_metrics.get(url, {})
        processing_list = [
            per_timestamp
            for per_source in processing_raw.values()
            for per_timestamp in per_source.values()
        ]

        transfer_raw = self.aggregated_transfer_metrics.get(url, {})
        transfer_list = [
            per_timestamp
            for per_source in transfer_raw.values()
            for per_client in per_source.values()
            for per_timestamp in per_client.values()
        ]

        return {
            "url": url,
            "processing": [m.to_dict() for m in processing_list],
            "transfer": [m.to_dict() for m in transfer_list],
        }







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
def get_nodes():
    return monitor.get_nodes_info()

@app.get("/nodes/{node_url}")
def get_node_metric(node_url: str):
    if node_url not in monitor.nodes:
        raise HTTPException(status_code=404, detail=f"Node '{node_url}' not found")

    return monitor.get_nodes_metrics(node_url)




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Monitor Node")
    parser.add_argument(
        "--flight-port",
        type=int,
        default=8000,
        help="Port to run the WoolMilk Monitor Node",
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=7999,
        help="Port to run the WoolMilk Monitor REST",
    )


    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Monitor Configuration")
    print("=" * 40)
    print(f" Flight gRPC Server : grpc://0.0.0.0:{args.flight_port}")
    print(f" REST API           : http://0.0.0.0:{args.api_port}")
    print("=" * 40 + "\n")

    monitor = MonitorNode(f"grpc://0.0.0.0:{args.flight_port}")

    try:
        flight_thread = threading.Thread(target= lambda: monitor.serve, daemon = True)
        flight_thread.start()
        uvicorn.run(app, host="0.0.0.0", port=args.api_port)

    except KeyboardInterrupt:
        print("Shutting down WoolMilk Monitor Node")
        monitor.shutdown()
        flight_thread.join()
        exit(0)


