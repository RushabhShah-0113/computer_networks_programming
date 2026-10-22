
import csv
import os
import threading
import time
from collections import deque


class TransferMetrics:
    """
    Part I performance metrics.

    Client records:
      - End-to-end completion time
      - Useful bytes
      - Received payload rate for ETA

    Server records:
      - FILE_DATA payload bytes successfully sent

    The experiment runner should combine client and server measurements
    into one result row.
    """

    def __init__(
        self,
        implementation="",
        topic="",
        client_count=1,
        trial=1,
    ):
        self.implementation = implementation
        self.topic = topic
        self.client_count = client_count
        self.trial = trial

        self.start_time = None
        self.end_time = None
        self.transfer_completed = False

        self.total_bytes_transmitted = 0
        self.useful_bytes = 0

        self._rate_samples = deque(maxlen=10)
        self._last_sample_time = None
        self._last_sample_bytes = 0
        self._lock = threading.Lock()

    def start(self):
        """Call immediately before sending the initial GET frame."""
        self.start_time = time.perf_counter()
        self._last_sample_time = self.start_time
        self._last_sample_bytes = 0

    def finish(self, transfer_complete_received=True):
        """Call when TRANSFER_COMPLETE is received, or transfer fails."""
        self.end_time = time.perf_counter()
        self.transfer_completed = transfer_complete_received

    def add_transmitted_bytes(self, count):
        """
        Call on the server after successfully sending a FILE_DATA frame.
        Count only the file-data bytes, excluding framing and metadata.
        """
        if count < 0:
            raise ValueError("Byte count cannot be negative")

        with self._lock:
            self.total_bytes_transmitted += count

    def set_useful_bytes(self, count):
        """Set final complete topic size once, after successful completion."""
        if count < 0:
            raise ValueError("Byte count cannot be negative")

        self.useful_bytes = count

    def add_received_payload(self, count):
        """Record newly received file payload bytes for ETA estimation."""
        if count < 0:
            raise ValueError("Byte count cannot be negative")

        now = time.perf_counter()

        with self._lock:
            self._last_sample_bytes += count

            if self._last_sample_time is None:
                self._last_sample_time = now
                return

            elapsed = now - self._last_sample_time

            if elapsed >= 0.5:
                rate_mbps = (
                    self._last_sample_bytes
                    * 8
                    / elapsed
                    / 1_000_000
                )

                self._rate_samples.append(rate_mbps)
                self._last_sample_time = now
                self._last_sample_bytes = 0

    def elapsed_seconds(self):
        if self.start_time is None:
            return 0.0

        end = (
            self.end_time
            if self.end_time is not None
            else time.perf_counter()
        )

        return max(end - self.start_time, 0.0)

    def redundant_bytes(self):
        return max(
            self.total_bytes_transmitted - self.useful_bytes,
            0,
        )

    def throughput_mbps(self):
        """Useful bytes divided by end-to-end elapsed time."""
        elapsed = self.elapsed_seconds()

        if elapsed <= 0 or not self.transfer_completed:
            return 0.0

        return self.useful_bytes * 8 / elapsed / 1_000_000

    def raw_payload_throughput_mbps(self):
        """Total FILE_DATA payload bytes divided by elapsed time."""
        elapsed = self.elapsed_seconds()

        if elapsed <= 0:
            return 0.0

        return (
            self.total_bytes_transmitted
            * 8
            / elapsed
            / 1_000_000
        )

    def moving_average_rate_mbps(self):
        with self._lock:
            if not self._rate_samples:
                return 0.0

            return sum(self._rate_samples) / len(self._rate_samples)

    def file_eta_seconds(self, remaining_bytes):
        if remaining_bytes <= 0:
            return 0.0

        rate_mbps = self.moving_average_rate_mbps()

        if rate_mbps <= 0:
            return None

        bytes_per_second = rate_mbps * 1_000_000 / 8
        return remaining_bytes / bytes_per_second

    def summary(self):
        return {
            "implementation": self.implementation,
            "topic": self.topic,
            "client_count": self.client_count,
            "trial": self.trial,
            "completion_time_seconds": round(
                self.elapsed_seconds(), 6
            ),
            "total_bytes_transmitted": self.total_bytes_transmitted,
            "useful_bytes": self.useful_bytes,
            "redundant_bytes": self.redundant_bytes(),
            "throughput_mbps": round(self.throughput_mbps(), 6),
            "raw_payload_throughput_mbps": round(
                self.raw_payload_throughput_mbps(), 6
            ),
            "status": (
                "success"
                if self.transfer_completed
                else "incomplete"
            ),
        }


def append_csv(path, row):
    """Append one row to CSV, creating parent directories if necessary."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

    file_exists = os.path.exists(path) and os.path.getsize(path) > 0

    with open(path, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(row.keys()))

        if not file_exists:
            writer.writeheader()

        writer.writerow(row)
