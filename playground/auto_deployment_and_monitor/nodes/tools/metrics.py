"""
    --- Metrics Implementation ---
    Contains Classes and Functions for utilizing Metrics
"""
import threading
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
    DOWN = "DOWN" # Never use it outside MonitorNode

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
            "status": self.status.value,
            "cpu": self.cpu,
            "memory": self.memory
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


class MonitorService:
    def __init__(self, metric_config: MetricConfig = None, url: str = "", log_level: LogType = LogType.WARN):
        self.proc = psutil.Process()
        self.proc.cpu_percent()
        self.proc.memory_percent()

        self.monitor_node = flight.FlightClient(url)
        self.monitor_connected = False
        self.lock = threading.Lock()
        self.metric_config = metric_config
        self._internal_error = False
        self.log_level = log_level
        self.logs: list[LogObject] = []
        self.oldLogs: list[LogObject] = []


    def check_health(self) -> HealthResult:
        cpu = self.proc.cpu_percent()
        memory = self.proc.memory_percent()

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

    def connect_monitor(self, url: str):
        try :
            results = self.monitor_node.do_action(
                flight.Action("register", url.encode("utf-8"))
            )
            result = next(results, None)

            if result:
                self.logs.append(LogObject("connect_monitor::Registered to Monitor", LogType.INFO))
                self.monitor_connected = True
            else:
                self.logs.append(
                    LogObject(f"connect_monitor::No Response from Monitor", LogType.WARN))


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
                    LogObject(f"disconnect_monitor::No Response from Monitor", LogType.WARN))
        except Exception as e:
            self.logs.append(LogObject(f"connect_monitor::Failed to disconnect to Monitor::{e}", LogType.ERROR))
            self._internal_error = True
            print("Failed to connect to monitor node")

    def parse_logs(self, all_logs: bool):
        logs = self.logs[:]
        self.logs.clear()
        self.oldLogs.extend(logs)
        return self.oldLogs if all_logs else logs
