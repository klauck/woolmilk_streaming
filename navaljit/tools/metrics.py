"""
    --- Metrics Implementation ---
    Contains Classes and Functions for utilizing Metrics
"""
from dataclasses import dataclass
from enum import Enum
from typing import Final

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
class HealthConfig:
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


class MetricType(str, Enum):
    RECEIVE = "RECEIVE"
    SEND = "SEND"  # also write in parquet file for sink
    PROCESS = "PROCESS"

class Metric:
    def __init__(self, duration_ns: float, nbytes: int, address: str, metric_type: MetricType):
        self.duration_ns = duration_ns
        self.bytes = nbytes
        self.address = address
        self.type = metric_type

    def to_dict(self):
        return {
            "address": self.address,
            "duration_ns": self.duration_ns,
            "bytes": self.bytes,
            "type": self.type
        }

