"""
    --- Logger Implementation ---
    Contains Classes and Functions for utilizing Logs
"""
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import IntEnum, Enum
from typing import Deque


class Colors:
    BLUE = '\033[34m'
    RED = '\033[31m'
    YELLOW = '\033[33m'
    RESET = '\033[0m'

class LogType(IntEnum):
    DEBUG = 1
    INFO = 2
    WARN = 3
    ERROR = 4

@dataclass
class LogObject:
    message: str
    type: LogType
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self):
        return {
            "message": self.message,
            "type": self.type.name,
            "timestamp": self.timestamp.isoformat()
        }

class LogService:
    def __init__(self, log_save_level: LogType = LogType.WARN, log_print_level: LogType = LogType.INFO, log_max: int = 1000):
        self.log_save_level = log_save_level
        self.log_print_level = log_print_level
        #self.logs: Deque[LogObject] = deque(maxlen=log_max)
        self.logs: Deque[str] = deque(maxlen=log_max)
        self._lock = threading.Lock()

    def log(self, message: str, level: LogType = LogType.DEBUG):
        if level >= self.log_save_level:
            with self._lock:
                #self.logs.append(LogObject(message, level))
                self.logs.append(message)

        if level >= self.log_print_level:
            self.print_log(message, level)

    def get_logs(self):
        with self._lock:
            logs = list(self.logs)
            self.logs.clear()
            return logs

    def print_log(self, message: str, level: LogType):
        if level == LogType.DEBUG:
            print(f"[DEBUG] {message}")
        elif level == LogType.INFO:
            print(f"[{Colors.BLUE}INFO{Colors.RESET}] {message}")
        elif level == LogType.WARN:
            print(f"[{Colors.YELLOW}WARN{Colors.RESET}] {message}")
        elif level == LogType.ERROR:
            print(f"[{Colors.RED}ERROR{Colors.RESET}] {message}")