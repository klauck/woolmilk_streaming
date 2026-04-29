"""
    --- Metrics Implementation ---
    Contains Classes and Functions for utilizing Metrics
"""
from dataclasses import dataclass
from enum import Enum
from typing import Final
import pyarrow as pa

UNDEFINED_MONITOR: Final[str] = "No Monitor configured for this node."

class HealthStatus(str, Enum):
    OK = "OK"
    WARN = "WARN"
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    UNKNOWN = "UNKNOWN"


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
    SEND = "SEND"
    PROCESS = "PROCESS"
    WRITE = "WRITE"

@dataclass
class Metric:
    client_url: str
    type: MetricType
    duration_ns: int
    bytes: int

METRIC_SCHEMA: pa.Schema = pa.schema([
    ("client_url", pa.string()),
    ("metric_type", pa.string()),
    ("duration_ns", pa.int64()),
    ("bytes", pa.int64()),
])

def metrics_to_record_batch(metrics: list[Metric]) -> pa.RecordBatch:

    address_list: list[str] = []
    type_list: list[str] = []
    duration_list: list[int] = []
    bytes_list: list[int] = []

    for metric in metrics:
        address_list.append(metric.client_url)
        type_list.append(metric.type.value)
        duration_list.append(metric.duration_ns)
        bytes_list.append(metric.bytes)

    list_arrays: list[pa.Array] = [
        pa.array(address_list),
        pa.array(type_list),
        pa.array(duration_list),
        pa.array(bytes_list)
    ]

    return pa.RecordBatch.from_arrays(list_arrays, schema=METRIC_SCHEMA)