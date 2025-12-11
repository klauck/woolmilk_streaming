"""
    --- Metrics Implementation ---
    Contains Classes and Functions for utilizing Metrics
"""
import json
import math
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Final

import psutil
from pyarrow import flight

UNDEFINED_MONITOR: Final[str] = "No Monitor configured for this node."

class HealthStatus(str, Enum):
    OK = "OK"
    WARN = "WARN"
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    DOWN = "UNKNOWN" # Never use it outside MonitorNode

@dataclass
class LIMIT:
    warn: float
    critical: float

@dataclass
class MetricConfig:
    cpu_limit: LIMIT
    mem_limit: LIMIT


@dataclass
class HealthResult:
    status: HealthStatus
    cpu: float
    memory: float

    def to_dict(self):
        return {
            "STATUS": self.status.value,
            "CPU": self.cpu,
            "MEM": self.memory
        }

class LogType(str, Enum):
    INFO = "INFO"
    DEBUG = "DEBUG"
    WARN = "WARN"
    ERROR = "ERROR"


@dataclass
class LogObject:
    message: str
    type: LogType
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self):
        return {
            "message": self.message,
            "type": self.type.value,
            "timestamp": self.timestamp.isoformat()
        }

@dataclass
class RawProcessingMetric:
    source: str
    duration: float
    size: int
    timestamp: float = field(default_factory=datetime.now)

    def from_dict(self: dict) -> "RawProcessingMetric":
        return RawProcessingMetric(
            source=self["source"],
            timestamp=float(self["timestamp"]),
            duration=float(self["duration"]),
            size=int(self["size"]),
        )

    def to_dict(self):
        return {
            "source": self.source,
            "timestamp": self.timestamp,
            "duration": self.duration,
            "size": self.size,
        }

@dataclass
class RawTransferMetric:
    source: str
    client: str
    duration: float
    size: int
    timestamp: float = field(default_factory=datetime.now)

    def from_dict(self: dict) -> "RawTransferMetric":
        return RawTransferMetric(
            source=self["source"],
            client=self["client"],
            timestamp=float(self["timestamp"]),
            duration=float(self["duration"]),
            size=int(self["size"]),
        )

    def to_dict(self):
        return {
            "source": self.source,
            "client": self.client,
            "timestamp": self.timestamp,
            "duration": self.duration,
            "size": self.size,
        }

@dataclass
class AggregatedProcessingMetric:
    source: str
    total_duration: float
    total_size: int
    timestamp: float = field(default_factory=datetime.now)
    count: int = 0
    def to_dict(self):
        return {
            "source": self.source,
            "total_duration": self.total_duration,
            "timestamp": self.timestamp,
            "total_size": self.total_size,
            "count": self.count,
        }


@dataclass
class AggregatedTransferMetric:
    source: str
    client: str
    total_size: int
    timestamp: float = field(default_factory=datetime.now)
    count: int = 0
    total_duration: float = 0.0
    def to_dict(self):
        return {
            "source": self.source,
            "client": self.client,
            "timestamp": self.timestamp,
            "total_duration": self.total_duration,
            "total_size": self.total_size,
            "count": self.count,
        }

class MonitorService:
    def __init__(self, metric_config: MetricConfig = None, url: str = "", log_level: LogType = LogType.WARN):
        self.proc = psutil.Process()
        self.proc.cpu_percent()
        self.proc.memory_percent()

        self.monitor_node = flight.FlightClient(url)
        self.raw_processing_metrics = []        # Important for Processing Nodes and Sink Nodes
        self.raw_transfer_metrics = []          # Important for Source and Processing Nodes
        self.monitor_connected = False
        self.lock = threading.Lock()
        self.metric_config = metric_config
        self._internal_error = False
        self.log_level = log_level
        self.logs: list[LogObject] = []
        self.oldLogs: list[LogObject] = []


    def check_health(self) -> HealthResult:
        cpu = round(self.proc.cpu_percent(), 2)
        memory = round(self.proc.memory_percent(), 2)

        if self.metric_config is None:
            self.logs.append(LogObject(
                "MonitorService::check_health::MetricConfig is not defined",
                LogType.ERROR))
            self._internal_error = True

        if self._internal_error:
            return HealthResult(HealthStatus.ERROR, cpu, memory)

        if (cpu >= self.metric_config.cpu_limit.critical
        or memory >= self.metric_config.mem_limit.critical):
            if self.log_level == LogType.WARN:
                self.logs.append(LogObject(
                    "MonitorService::check_health::MetricConfig CPU limit exceeded",
                    LogType.WARN
                ))
            return HealthResult(HealthStatus.CRITICAL, cpu, memory)

        if (cpu >= self.metric_config.cpu_limit.warn
        or memory >= self.metric_config.mem_limit.warn):
            if self.log_level == LogType.WARN:
                self.logs.append(LogObject(
                    "MonitorService::check_health::MetricConfig CPU Warn exceeded",
                    LogType.WARN
                ))
            return HealthResult(HealthStatus.WARN, cpu, memory)

        return HealthResult(HealthStatus.OK, cpu, memory)

    def connect_monitor(self, url: str, forward_url: list[str] | None = None, query: str | None = None):
        try :

            payload = {
                "URL": url,
                "ForwardURLS": forward_url or [],
                "QUERY": query or ""
            }

            results = self.monitor_node.do_action(
                flight.Action("register", json.dumps(payload).encode("utf-8"))
            )
            result = next(results, None)

            if result:
                self.logs.append(LogObject("connect_monitor::Registered to Monitor", LogType.INFO))
                self.monitor_connected = True
            else:
                self.logs.append(
                    LogObject("connect_monitor::No Response from Monitor", LogType.WARN))


        except Exception as e:
            self.logs.append(LogObject(f"connect_monitor::Failed to connect to Monitor::{e}", LogType.ERROR))
            self._internal_error = True
            print("Failed to connect to monitor node")

    def disconnect_monitor(self, url: str):
        try:
            results = self.monitor_node.do_action(
                flight.Action("disconnect", url.encode("utf-8"))
            )
            result = next(results, None)

            if result:
                self.logs.append(LogObject("disconnect_monitor::Disconnected to Monitor", LogType.INFO))

            else:
                self.logs.append(
                    LogObject("disconnect_monitor::No Response from Monitor", LogType.WARN))
        except Exception as e:
            self.logs.append(LogObject(f"connect_monitor::Failed to disconnect to Monitor::{e}", LogType.ERROR))
            self._internal_error = True
            print("Failed to connect to monitor node")

    def parse_logs(self, all_logs: bool):
        logs = self.logs[:]
        self.logs.clear()
        self.oldLogs.extend(logs)
        return self.oldLogs if all_logs else logs


    def parse_metrics(self):
        proc = [m.to_dict() for m in self.raw_processing_metrics]
        trans = [m.to_dict() for m in self.raw_transfer_metrics]

        self.raw_processing_metrics.clear()
        self.raw_transfer_metrics.clear()
        return {
            "processing_metrics": proc,
            "transfer_metrics": trans,
        }



class NodeInfo:
    def __init__(self, url: str, forward_urls: list[str] | None = None, query: str = ""):
        self.url = url
        self.forward_urls = forward_urls or []
        self.query = query
        self.client = flight.FlightClient(f"grpc://{url}")


def str2bool(v: str) -> bool:
    return v.lower() in ("True", "true")