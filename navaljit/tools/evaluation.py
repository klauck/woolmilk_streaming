import datetime
import json
import os
import time
import threading
from dataclasses import dataclass

import psutil

from tools.metrics import MetricType


@dataclass
class ResourceMetric:
    timestamp_ns: int
    cpu_percent: float
    memory_mb: float


@dataclass
class EventMetric:
    timestamp_ns: int
    event_type: MetricType
    duration: int
    batch_bytes: int
    rows: int


class OverheadEvaluation:
    def __init__(self, interval_sec: float = 1.0):
        self.interval_sec = interval_sec
        self.process = psutil.Process(os.getpid())
        self.running = False
        self.thread = None

        self.resources: list[ResourceMetric] = []
        self.events: list[EventMetric] = []

        self.start_time = 0
        self.end_time = 0

        self.lock = threading.Lock()

    def start(self):
        self.running = True
        self.process.cpu_percent(interval=None)
        self.start_time = time.time_ns()

        self.thread = threading.Thread(target=self._sample_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        self.end_time = time.time_ns()
        self.thread.join(timeout=2)

    def _sample_loop(self):
        while self.running:
            cpu_percent = self.process.cpu_percent(interval=None)
            mem_mb = self.process.memory_info().rss / (1024 ** 2)

            with self.lock:
                self.resources.append(ResourceMetric(time.time_ns(), cpu_percent, mem_mb))

            time.sleep(self.interval_sec)

    def add_event_metric(self, event_type: MetricType, duration: int, batch_bytes: int, rows: int):
        with self.lock:
            self.events.append(EventMetric(time.time_ns(), event_type, duration, batch_bytes, rows))


    def summary(self) -> dict:
        with self.lock:
            duration_ns = self.end_time - self.start_time

            avg_cpu = (
                sum(r.cpu_percent for r in self.resources) / len(self.resources)
            )
            max_cpu = max((r.cpu_percent for r in self.resources))

            avg_memory_mb = (
                sum(r.memory_mb for r in self.resources) / len(self.resources)
            )
            max_memory_mb = max((r.memory_mb for r in self.resources))

            event_summary = {}

            for event_type in MetricType:
                matching = [e for e in self.events if e.event_type == event_type]
                if not matching:
                    continue

                total_duration_ns = sum(e.duration for e in matching)
                total_bytes = sum(e.batch_bytes for e in matching)
                total_rows = sum(e.rows for e in matching)
                count = len(matching)

                avg_duration_ns = total_duration_ns / count
                avg_bytes = total_bytes / count
                avg_rows = total_rows / count

                bandwidth = (total_bytes * (10 ** 3)) / total_duration_ns

                event_summary[event_type.name] = {
                    "count": count,
                    "total_duration_ns": total_duration_ns,
                    "avg_duration_ns": avg_duration_ns,
                    "total_bytes": total_bytes,
                    "avg_bytes": avg_bytes,
                    "total_rows": total_rows,
                    "avg_rows": avg_rows,
                    "bandwidth": bandwidth
                }

            return {
                "duration": duration_ns,
                "resource_summary": {
                    "avg_cpu_percent": avg_cpu,
                    "max_cpu_percent": max_cpu,
                    "avg_memory_mb": avg_memory_mb,
                    "max_memory_mb": max_memory_mb,
                    "num_resource_samples": len(self.resources),
                },
                "event_summary": event_summary,
                "num_event_samples": len(self.events),
            }

    def create_file(self, path: str):
        path = f"evaluation/{path}.json"

        entry = self.summary()

        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    data = []
        else:
            data = []

        if not isinstance(data, list):
            data = [data]

        data.append(entry)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)