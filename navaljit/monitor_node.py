"""
    --- Woolmilk Monitor Node ---
    Monitors all Nodes (Sink, Processing and Sources) and provides REST for UI
"""

import argparse
import json
import threading
import time
from queue import Queue

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pyarrow import flight

from tools.logger import LogObject
from tools.monitor import NodeInfo


# ==========================
# APACHE ARROW FLIGHTSERVER
# ==========================

class MonitorNode(flight.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)
        self.nodes: dict[str, NodeInfo] = {}
        self.lock = threading.Lock()
        self.poll_thread = threading.Thread(target=self.poll_logs, args=(), daemon=True)
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
            query: str = data.get("query", "")

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

    def poll_logs(self):
        while True:
            with self.lock:
                nodes_copy = list(self.nodes.items())

            for url, node_info in nodes_copy:
                try:
                    client: flight.FlightClient = node_info.client
                    res = next(client.do_action(flight.Action("logs", b"")), None)
                    if res is None: continue
                    payload = json.loads(res.body.to_pybytes().decode("utf-8"))
                    print(f"Poll Logs: {url}: {payload}")

                except Exception as e:
                    print(f"[MonitorNode] Failed to poll metrics from {url}: {e}")


            time.sleep(5)

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


