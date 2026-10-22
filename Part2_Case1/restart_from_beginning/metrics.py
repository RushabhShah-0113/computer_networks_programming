import csv
import os
import threading
import time
from collections import deque


class TransferMetrics:
    def __init__(self, topic="", implementation="case1_restart"):
        self.topic = topic
        self.implementation = implementation

        self.start_time = None
        self.end_time = None
        self.success = False

        self.total_bytes_transmitted = 0
        self.useful_bytes = 0
        self.failure_count = 0
        self.reconnect_count = 0

        self._rates = deque(maxlen=10)
        self._sample_start = None
        self._sample_bytes = 0
        self._lock = threading.Lock()

    def start(self):
        self.start_time = time.perf_counter()
        self._sample_start = self.start_time

    def finish(self, success):
        self.end_time = time.perf_counter()
        self.success = success

    def add_transmitted_bytes(self, count):
        with self._lock:
            self.total_bytes_transmitted += count

    def add_received_bytes(self, count):
        """Use received payload rate for ETA; this is not server byte accounting."""
        now = time.perf_counter()

        with self._lock:
            self._sample_bytes += count

            if self._sample_start is None:
                self._sample_start = now
                return

            elapsed = now - self._sample_start
            if elapsed >= 0.5:
                rate_mbps = self._sample_bytes * 8 / elapsed / 1_000_000
                self._rates.append(rate_mbps)
                self._sample_start = now
                self._sample_bytes = 0

    def add_failure(self):
        self.failure_count += 1

    def add_reconnect(self):
        self.reconnect_count += 1

    def elapsed_seconds(self):
        if self.start_time is None:
            return 0.0

        end = self.end_time if self.end_time is not None else time.perf_counter()
        return max(0.0, end - self.start_time)

    def redundant_bytes(self):
        return max(0, self.total_bytes_transmitted - self.useful_bytes)

    def throughput_mbps(self):
        elapsed = self.elapsed_seconds()
        if not self.success or elapsed <= 0:
            return 0.0
        return self.useful_bytes * 8 / elapsed / 1_000_000

    def raw_payload_throughput_mbps(self):
        elapsed = self.elapsed_seconds()
        if elapsed <= 0:
            return 0.0
        return self.total_bytes_transmitted * 8 / elapsed / 1_000_000

    def file_eta_seconds(self, remaining_bytes):
        if remaining_bytes <= 0:
            return 0.0

        with self._lock:
            rate = sum(self._rates) / len(self._rates) if self._rates else 0.0

        if rate <= 0:
            return None

        return remaining_bytes / (rate * 1_000_000 / 8)

    def summary(self):
        return {
            "implementation": self.implementation,
            "topic": self.topic,
            "completion_time_seconds": round(self.elapsed_seconds(), 6),
            "total_bytes_transmitted": self.total_bytes_transmitted,
            "useful_bytes": self.useful_bytes,
            "redundant_bytes": self.redundant_bytes(),
            "throughput_mbps": round(self.throughput_mbps(), 6),
            "raw_payload_throughput_mbps": round(
                self.raw_payload_throughput_mbps(), 6
            ),
            "checkpoint_overhead_bytes": 0,
            "failure_count": self.failure_count,
            "reconnect_count": self.reconnect_count,
            "status": "success" if self.success else "failed",
        }


def append_csv(path, row):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    exists = os.path.exists(path) and os.path.getsize(path) > 0

    with open(path, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)