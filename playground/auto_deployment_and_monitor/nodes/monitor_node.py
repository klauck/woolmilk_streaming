"""
    --- Woolmilk Monitor Node ---
    Monitors all Nodes (Sink, Processing and Sources) and provides REST for UI
"""
import argparse
import json
import threading

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.openapi.models import Response
from pyarrow import flight

from tools.metrics import HealthResult, HealthStatus

# ==========================
# APACHE ARROW FLIGHTSERVER
# ==========================

class MonitorNode(flight.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)
        self.clients: dict[str, flight.FlightClient] = {}
        self.lock = threading.Lock()

    def do_action(self, context, action):
        if action.type == "register":
            node_url:str = action.body.to_pybytes().decode("utf-8")
            with self.lock:
                if node_url not in self.clients:
                    print(f"Registering new client for {node_url}")
                    self.clients[node_url] = flight.FlightClient(f"grpc://{node_url}")
                else:
                    print(f"Client {node_url} has reconnected.")

            yield flight.Result(b"OK")

        elif action.type == "disconnect":
            node_url = action.body.to_pybytes().decode("utf-8")
            with self.lock:
                if node_url in self.clients:
                    del self.clients[node_url]
                    print(f"Disconnected node {node_url}")
            yield flight.Result(b"Disconnected")

    def check_health_client(self, client: flight.FlightClient) ->  HealthResult:
        try:
            raw = client.do_action(flight.Action("health", b""))
            res = next(raw, None)

            if res is None:
                return HealthResult(HealthStatus.DOWN, 0, 0)

            status = json.loads(res.body.to_pybytes().decode("utf-8"))
            return HealthResult(HealthStatus(status["status"]), status["cpu"], status["memory"])

        except (flight.FlightUnavailableError, flight.FlightTimedOutError, flight.FlightCancelledError):
            return HealthResult(HealthStatus.DOWN, 0, 0)

        except Exception as e:
            print(f"Health check failed: {e}")
            return HealthResult(HealthStatus.DOWN, 0, 0)


    def check_health(self) -> list[dict]:
        result: list[dict] = []
        with self.lock:
            items = self.clients

        for url, client in items.items():
            health = self.check_health_client(client)
            result.append({"url": url, **health.to_dict()})

        return result

    def disconnect(self, url: str) -> int:
        with self.lock:
            items = self.clients

        if url in items:
            client = items[url]
        else:
            return 1


        with self.lock:
            del self.clients[url]

        try:
            raw = client.do_action(flight.Action("shutdown", b""))
            res = next(raw, None)
            if res is None:
                return 1

        except Exception as e:
            print(f"Disconnect failed: {e}")
            return 1

        return 0



# ======================
# REST-API mit FastAPI
# ======================

monitor: MonitorNode = None
app = FastAPI()

@app.get("/nodes")
def get_nodes():
    with monitor.lock:
        return list(monitor.clients.keys())

@app.get("/health")
def get_health():
    return monitor.check_health()

@app.delete("/node/")
def delete_node(url: str):
    status = monitor.disconnect(url)
    if status == 0:
        return None
    elif status == 1:
        raise HTTPException(status_code=404, detail=f"Node {url} not found")
    else:
        raise HTTPException(status_code=500, detail=f"Node {url} could not be deleted")


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
