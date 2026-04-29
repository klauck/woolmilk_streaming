"""
    --- Monitor Implementation ---
    Contains Classes and Functions for utilizing Monitor
"""

import json
import threading
from enum import Enum
from queue import Queue

import psutil
from pyarrow import flight

from .metrics import HealthConfig, Metric, HealthResult, HealthStatus
from .logger import LogService, LogType

class NodeType(str, Enum):
    SOURCE = "Source"
    SINK = "Sink"
    PROCESS = "Processing"

class NodeInfo:
    def __init__(self, url: str, node_type: NodeType, forward_urls: list[str] | None = None, query: str = ""):
        self.forward_urls = forward_urls or []
        self.query = query
        self.node_type = node_type
        self.client = flight.FlightClient(f"grpc://{url}")

class MonitorService:
    def __init__(self, logger: LogService, health_config: HealthConfig = None, monitor_url: str = ""):
        self.proc = psutil.Process()
        self.proc.cpu_percent()
        self.proc.memory_percent()

        self._lock = threading.Lock()

        self.monitor_node = flight.FlightClient(monitor_url)
        self.health_config = health_config
        self.metric_lock = threading.Lock()
        self.metric_queue: Queue[Metric] = Queue()
        self.logger = logger
        self.monitor_connected = False
        self._internal_error = False

    def connect_monitor(self, url: str, node_type: NodeType, forward_url: list[str] | None = None, query: str | None = None):
        try :
            payload = {
                "url": url,
                "type": node_type,
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
            options = flight.FlightCallOptions(timeout=2.0)
            results = self.monitor_node.do_action(
                flight.Action("disconnect", url.encode("utf-8")),
                options
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


    def check_health(self) -> HealthResult:
        cpu = round(self.proc.cpu_percent() / psutil.cpu_count(), 2)
        memory = round(self.proc.memory_percent(), 2)
        memory_bytes = self.proc.memory_info().rss
        memory_mb = round(memory_bytes / (1024 * 1024), 2)

        if self.health_config is None:
            self.logger.log("MonitorService::check_health::MetricConfig is not defined", LogType.ERROR)
            self._internal_error = True

        if self._internal_error:
            return HealthResult(HealthStatus.ERROR, cpu, memory_mb)

        if (cpu >= self.health_config.cpu_limit.critical
        or memory >= self.health_config.mem_limit.critical):
            self.logger.log("MonitorService::check_health::MetricConfig CPU limit exceeded", LogType.WARN)
            return HealthResult(HealthStatus.CRITICAL, cpu, memory_mb)

        if (cpu >= self.health_config.cpu_limit.warn
        or memory >= self.health_config.mem_limit.warn):
            self.logger.log("MonitorService::check_health::MetricConfig CPU Warn exceeded", LogType.WARN)
            return HealthResult(HealthStatus.WARN, cpu, memory_mb)

        return HealthResult(HealthStatus.OK, cpu, memory_mb)