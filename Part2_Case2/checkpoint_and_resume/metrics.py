
import csv
import os
import time
from threading import Lock


class TransferMetrics:
    def __init__(self, topic, implementation="blocking"):
        self.topic = topic
        self.implementation = implementation

        self.start_time = None
        self.end_time = None

        # Byte counters
        self.total_bytes_transmitted = 0
        self.useful_bytes = 0
        self.checkpoint_overhead_bytes = 0

        # Event counters
        self.failure_count = 0
        self.reconnect_count = 0
        self.checkpoint_count = 0

        # Timing counters
        self.checkpoint_processing_seconds = 0.0

        # Per-file progress for ETA
        self.file_progress = {}
        self.file_rates = {}
        self._last_progress_time = {}
        self._last_progress_bytes = {}

        self._lock = Lock()

    def start(self):
        """Start measuring the complete transfer."""
        with self._lock:
            if self.start_time is None:
                self.start_time = time.perf_counter()

    def finish(self):
        """Mark completion after TRANSFER_COMPLETE is received."""
        with self._lock:
            self.end_time = time.perf_counter()

    @property
    def elapsed_seconds(self):
        if self.start_time is None:
            return 0.0

        end = self.end_time
        if end is None:
            end = time.perf_counter()

        return max(0.0, end - self.start_time)

    @property
    def redundant_bytes(self):
        return max(
            0,
            self.total_bytes_transmitted - self.useful_bytes,
        )

    @property
    def throughput_mbps(self):
        """Useful throughput in decimal megabits per second."""
        elapsed = self.elapsed_seconds
        if elapsed <= 0:
            return 0.0

        return (self.useful_bytes * 8) / elapsed / 1_000_000

    @property
    def raw_payload_throughput_mbps(self):
        """Transmitted file-payload throughput, including retransmissions."""
        elapsed = self.elapsed_seconds
        if elapsed <= 0:
            return 0.0

        return (
            self.total_bytes_transmitted * 8
        ) / elapsed / 1_000_000

    def add_transmitted_bytes(self, count):
        """Record actual file-data bytes sent by the server."""
        if count < 0:
            raise ValueError("Byte count cannot be negative")

        with self._lock:
            self.total_bytes_transmitted += count

    def add_useful_bytes(self, count):
        """Record newly completed file bytes, counted only once."""
        if count < 0:
            raise ValueError("Byte count cannot be negative")

        with self._lock:
            self.useful_bytes += count

    def add_checkpoint_overhead(self, count):
        """Record bytes used by checkpoint protocol frames."""
        if count < 0:
            raise ValueError("Byte count cannot be negative")

        with self._lock:
            self.checkpoint_overhead_bytes += count

    def record_checkpoint(self, processing_seconds=0.0):
        """Record a processed checkpoint acknowledgement."""
        with self._lock:
            self.checkpoint_count += 1
            self.checkpoint_processing_seconds += max(
                0.0, processing_seconds
            )

    def record_failure(self):
        """Record a failed connection or injected disconnect."""
        with self._lock:
            self.failure_count += 1

    def record_reconnect(self):
        """Call after a recovery connection has been established."""
        with self._lock:
            self.reconnect_count += 1

    def update_file_progress(self, filename, confirmed_bytes):
        """
        Update progress using the server-confirmed offset.

        ETA is based on the moving-average rate of confirmed progress.
        """
        now = time.perf_counter()

        with self._lock:
            previous_time = self._last_progress_time.get(filename)
            previous_bytes = self._last_progress_bytes.get(filename)

            self.file_progress[filename] = max(0, confirmed_bytes)

            if previous_time is not None and previous_bytes is not None:
                dt = now - previous_time
                db = confirmed_bytes - previous_bytes

                if dt > 0 and db > 0:
                    instantaneous_rate = db / dt
                    old_rate = self.file_rates.get(filename)

                    if old_rate is None:
                        self.file_rates[filename] = instantaneous_rate
                    else:
                        # Exponential moving average
                        alpha = 0.3
                        self.file_rates[filename] = (
                            alpha * instantaneous_rate
                            + (1 - alpha) * old_rate
                        )

            self._last_progress_time[filename] = now
            self._last_progress_bytes[filename] = confirmed_bytes

    def file_eta_seconds(self, filename, file_size):
        """Estimate remaining time for a file in seconds."""
        with self._lock:
            progress = self.file_progress.get(filename, 0)
            rate = self.file_rates.get(filename, 0.0)

        remaining = max(0, file_size - progress)

        if remaining == 0:
            return 0.0

        if rate <= 0:
            return None

        return remaining / rate

    def summary(self):
        return {
            "topic": self.topic,
            "implementation": self.implementation,
            "elapsed_seconds": round(self.elapsed_seconds, 6),
            "total_bytes_transmitted": self.total_bytes_transmitted,
            "useful_bytes": self.useful_bytes,
            "redundant_bytes": self.redundant_bytes,
            "throughput_mbps": round(self.throughput_mbps, 6),
            "raw_payload_throughput_mbps": round(
                self.raw_payload_throughput_mbps, 6
            ),
            "checkpoint_overhead_bytes": self.checkpoint_overhead_bytes,
            "checkpoint_count": self.checkpoint_count,
            "checkpoint_processing_seconds": round(
                self.checkpoint_processing_seconds, 6
            ),
            "failure_count": self.failure_count,
            "reconnect_count": self.reconnect_count,
        }

    def append_csv(self, csv_path):
        """Append one metrics record to a CSV file."""
        result = self.summary()
        directory = os.path.dirname(csv_path)

        if directory:
            os.makedirs(directory, exist_ok=True)

        file_exists = (
            os.path.exists(csv_path)
            and os.path.getsize(csv_path) > 0
        )

        with open(csv_path, "a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=list(result.keys()),
            )

            if not file_exists:
                writer.writeheader()

            writer.writerow(result)
