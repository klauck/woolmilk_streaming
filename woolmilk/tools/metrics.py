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