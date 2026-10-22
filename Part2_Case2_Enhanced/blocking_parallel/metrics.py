
import csv
import os
import threading
import time


class TransferMetrics:
    def __init__(self, topic, implementation="blocking_parallel"):
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

        # Checkpoint processing duration, in seconds
        self.checkpoint_processing_seconds = 0.0

        # Per-file progress and rate estimates
        self.file_progress = {}
        self.file_rates = {}
        self.file_samples = {}

        self.lock = threading.Lock()

    # --------------------------------------------------
    # Timer
    # --------------------------------------------------

    def start(self):
        """Start timing immediately before the initial GET."""
        with self.lock:
            if self.start_time is None:
                self.start_time = time.perf_counter()

    def finish(self):
        """Stop timing after the topic is completely received."""
        with self.lock:
            if self.end_time is None:
                self.end_time = time.perf_counter()

    @property
    def elapsed_seconds(self):
        with self.lock:
            if self.start_time is None:
                return 0.0

            end = (
                self.end_time
                if self.end_time is not None
                else time.perf_counter()
            )

            return max(0.0, end - self.start_time)

    # --------------------------------------------------
    # Byte counters
    # --------------------------------------------------

    def record_transmitted_bytes(self, count):
        """
        Record FILE_DATA payload bytes sent by the server.
        Include retransmitted payload bytes.
        """
        if count < 0:
            raise ValueError("Byte count cannot be negative")

        with self.lock:
            self.total_bytes_transmitted += count

    def record_useful_bytes(self, count):
        """Count successfully completed file bytes exactly once."""
        if count < 0:
            raise ValueError("Byte count cannot be negative")

        with self.lock:
            self.useful_bytes += count

    def record_checkpoint_overhead(self, count):
        """Record the full byte size of checkpoint protocol frames."""
        if count < 0:
            raise ValueError("Byte count cannot be negative")

        with self.lock:
            self.checkpoint_overhead_bytes += count

    @property
    def redundant_bytes(self):
        with self.lock:
            return max(
                0,
                self.total_bytes_transmitted - self.useful_bytes,
            )

    # --------------------------------------------------
    # Failures and recovery
    # --------------------------------------------------

    def record_failure(self):
        """Record an injected connection failure."""
        with self.lock:
            self.failure_count += 1

    def record_reconnect(self):
        """Record a successfully established recovery connection."""
        with self.lock:
            self.reconnect_count += 1

    def record_checkpoint(self, processing_seconds):
        """Record a processed checkpoint acknowledgement."""
        with self.lock:
            self.checkpoint_count += 1
            self.checkpoint_processing_seconds += max(
                0.0, processing_seconds
            )

    # --------------------------------------------------
    # Per-file ETA
    # --------------------------------------------------

    def update_file_progress(self, filename, confirmed_offset):
        """
        Update the rate estimate using server-confirmed progress.

        A failed transfer should not advance confirmed_offset.
        """
        now = time.perf_counter()

        with self.lock:
            previous = self.file_samples.get(filename)

            self.file_progress[filename] = confirmed_offset

            if previous is not None:
                previous_offset, previous_time = previous

                elapsed = now - previous_time
                transferred = confirmed_offset - previous_offset

                if elapsed > 0 and transferred > 0:
                    sample_rate = transferred / elapsed
                    old_rate = self.file_rates.get(filename)

                    if old_rate is None:
                        self.file_rates[filename] = sample_rate
                    else:
                        alpha = 0.3
                        self.file_rates[filename] = (
                            alpha * sample_rate
                            + (1 - alpha) * old_rate
                        )

            self.file_samples[filename] = (
                confirmed_offset,
                now,
            )

    def reset_file_eta(self, filename):
        """Clear the rate estimate after a file connection fails."""
        with self.lock:
            self.file_rates.pop(filename, None)
            self.file_samples.pop(filename, None)

    def file_eta_seconds(self, filename, file_size):
        """Return ETA in seconds, or None if no rate is available."""
        with self.lock:
            confirmed = self.file_progress.get(filename, 0)
            rate = self.file_rates.get(filename, 0.0)

        remaining = max(0, file_size - confirmed)

        if remaining == 0:
            return 0.0

        if rate <= 0:
            return None

        return remaining / rate

    # --------------------------------------------------
    # Throughput
    # --------------------------------------------------

    @property
    def throughput_mbps(self):
        """Useful throughput in decimal megabits per second."""
        elapsed = self.elapsed_seconds

        if elapsed <= 0:
            return 0.0

        with self.lock:
            useful = self.useful_bytes

        return useful * 8 / elapsed / 1_000_000

    @property
    def raw_payload_throughput_mbps(self):
        """Raw FILE_DATA payload rate, including retransmissions."""
        elapsed = self.elapsed_seconds

        if elapsed <= 0:
            return 0.0

        with self.lock:
            transmitted = self.total_bytes_transmitted

        return transmitted * 8 / elapsed / 1_000_000

    # --------------------------------------------------
    # Reporting
    # --------------------------------------------------

    def summary(self):
        with self.lock:
            total = self.total_bytes_transmitted
            useful = self.useful_bytes
            overhead = self.checkpoint_overhead_bytes
            failures = self.failure_count
            reconnects = self.reconnect_count
            checkpoints = self.checkpoint_count
            processing = self.checkpoint_processing_seconds

        return {
            "topic": self.topic,
            "implementation": self.implementation,
            "completion_time_seconds": round(
                self.elapsed_seconds, 6
            ),
            "total_bytes_transmitted": total,
            "useful_bytes": useful,
            "redundant_bytes": max(0, total - useful),
            "throughput_mbps": round(self.throughput_mbps, 6),
            "raw_payload_throughput_mbps": round(
                self.raw_payload_throughput_mbps, 6
            ),
            "checkpoint_overhead_bytes": overhead,
            "checkpoint_count": checkpoints,
            "checkpoint_processing_seconds": round(processing, 6),
            "failure_count": failures,
            "reconnect_count": reconnects,
        }

    def append_csv(self, csv_path):
        """Append one transfer summary to a CSV file."""
        result = self.summary()

        directory = os.path.dirname(csv_path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        file_exists = (
            os.path.isfile(csv_path)
            and os.path.getsize(csv_path) > 0
        )

        with open(
            csv_path, "a", newline="", encoding="utf-8"
        ) as file:
            writer = csv.DictWriter(
                file,
                fieldnames=list(result.keys()),
            )

            if not file_exists:
                writer.writeheader()

            writer.writerow(result)
