"""A tiny thread-safe, in-memory event log.

The master pushes human-readable events here (task assigned, worker failed,
task reassigned, ...). The UI polls them to render the live activity feed.
Each event carries a monotonically increasing sequence number so the client
can ask for "everything after seq N".
"""
from __future__ import annotations

import threading
import time
from collections import deque
from typing import Deque, Dict, List

# event levels drive the colour of the dot in the UI
INFO = "info"
SUCCESS = "success"
WARN = "warn"
ERROR = "error"


class LogBus:
    def __init__(self, capacity: int = 500) -> None:
        self._lock = threading.Lock()
        self._events: Deque[Dict] = deque(maxlen=capacity)
        self._seq = 0

    def emit(self, level: str, message: str) -> None:
        with self._lock:
            self._seq += 1
            self._events.append(
                {
                    "seq": self._seq,
                    "ts": time.time(),
                    "level": level,
                    "message": message,
                }
            )

    def info(self, msg: str) -> None:
        self.emit(INFO, msg)

    def success(self, msg: str) -> None:
        self.emit(SUCCESS, msg)

    def warn(self, msg: str) -> None:
        self.emit(WARN, msg)

    def error(self, msg: str) -> None:
        self.emit(ERROR, msg)

    def after(self, seq: int) -> List[Dict]:
        with self._lock:
            return [e for e in self._events if e["seq"] > seq]

    def clear(self) -> None:
        with self._lock:
            self._events.clear()
