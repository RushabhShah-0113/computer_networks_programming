"""Generate standardized test datasets for benchmarking and live evaluation."""

import os
from pathlib import Path


def create_file(path, size_bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        # Write deterministic chunk pattern for fast generation and integrity checking
        chunk = b"A" * 65536
        remaining = size_bytes
        while remaining > 0:
            to_write = min(remaining, len(chunk))
            f.write(chunk[:to_write])
            remaining -= to_write


def generate_all_topics(topics_root):
    root = Path(topics_root).resolve()
    root.mkdir(parents=True, exist_ok=True)

    # 1. Topic for Part 1 Scalability (5 files x 1 MB = 5 MB)
    topic1 = root / "topic_scalability"
    for i in range(1, 6):
        create_file(topic1 / f"file_{i:02d}.dat", 1 * 1024 * 1024)

    # 2. Topic for Part 2 Fault Tolerance & Checkpoint Interval (4 files x 4 MB = 16 MB)
    topic2 = root / "topic_fault"
    for i in range(1, 5):
        create_file(topic2 / f"file_{i:02d}.dat", 4 * 1024 * 1024)

    # 3. Topic for Part 2 Enhanced Parallel Connections (8 files x 2 MB = 16 MB)
    topic3 = root / "topic_parallel"
    for i in range(1, 9):
        create_file(topic3 / f"file_{i:02d}.dat", 2 * 1024 * 1024)

    # 4. Large Topic for Live 3-Computer Evaluation (3 files x 10 MB = 30 MB)
    topic_live = root / "topic_live_demo"
    for i in range(1, 4):
        create_file(topic_live / f"video_{i:02d}.mp4", 10 * 1024 * 1024)

    print(f"[SUCCESS] Generated test topics under: {root}")
    for item in sorted(root.iterdir()):
        if item.is_dir():
            files = list(item.glob("*"))
            total_size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
            print(f"  - {item.name}: {len(files)} files, {total_size_mb:.1f} MB total")


if __name__ == "__main__":
    generate_all_topics("test_topics")
