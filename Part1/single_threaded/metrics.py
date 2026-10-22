
"""Performance measurement helpers for Part I."""

import csv
import os
import time


class TransferMetrics:
    def __init__(self):
        self.start_time = time.perf_counter()
        self.payload_bytes = 0

    def add_bytes(self, count):
        self.payload_bytes += count

    def elapsed_seconds(self):
        return max(time.perf_counter() - self.start_time, 1e-9)

    def throughput_mbps(self):
        # Decimal megabits per second.
        return (self.payload_bytes * 8) / self.elapsed_seconds() / 1_000_000

    def summary(self):
        return {
            "elapsed_seconds": round(self.elapsed_seconds(), 6),
            "payload_bytes": self.payload_bytes,
            "throughput_mbps": round(self.throughput_mbps(), 6),
        }


def append_csv(path, row):
    """Append one metrics row, writing a header for a new CSV."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    file_exists = os.path.exists(path) and os.path.getsize(path) > 0

    with open(path, "a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(row.keys()))

        if not file_exists:
            writer.writeheader()

        writer.writerow(row)
