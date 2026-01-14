"""
    --- Monitor Implementation ---
    Contains Classes and Functions for utilizing Monitor
"""

import json
import threading

import psutil
from pyarrow import flight

from .metrics import MetricConfig
from .logger import LogService, LogType

class NodeInfo:
    def __init__(self, url: str, forward_urls: list[str] | None = None, query: str = ""):
        self.forward_urls = forward_urls or []
        self.query = query
        self.client = flight.FlightClient(f"grpc://{url}")

class MonitorService:
    def __init__(self, logger: LogService, metric_config: MetricConfig = None, monitor_url: str = ""):
        self.proc = psutil.Process()
        self.proc.cpu_percent()
        self.proc.memory_percent()

        self._lock = threading.Lock()

        self.monitor_node = flight.FlightClient(monitor_url)
        self.metric_config = metric_config
        self.logger = logger
        self.monitor_connected = False
        self._internal_error = False

    def connect_monitor(self, url: str, forward_url: list[str] | None = None, query: str | None = None):
        try :
            payload = {
                "url": url,
                "forward_urls": forward_url or [],
                "query": query or ""
            }

            results = self.monitor_node.do_action(
                flight.Action("register", json.dumps(payload).encode("utf-8"))
            )
            result = next(results, None)

            if result:
                self.logger.log("connect_monitor::Registered to Monitor", LogType.INFO)
                with self._lock:
                    self.monitor_connected = True
            else:
                self.logger.log("connect_monitor::No Response from Monitor", LogType.WARN)

        except Exception as e:
            self.logger.log(f"connect_monitor::Failed to connect to Monitor::{e}", LogType.ERROR)
            self._internal_error = True

    def disconnect_monitor(self, url: str):
        try:
            results = self.monitor_node.do_action(
                flight.Action("disconnect", url.encode("utf-8"))
            )
            result = next(results, None)

            if result:
                self.logger.log("disconnect_monitor::Disconnected to Monitor", LogType.INFO)
                with self._lock:
                    self.monitor_connected = False
            else:
                self.logger.log("disconnect_monitor::No Response from Monitor", LogType.WARN)
        except Exception as e:
            self.logger.log(f"connect_monitor::Failed to disconnect to Monitor::{e}", LogType.ERROR)
            self._internal_error = True