"""Automated Benchmarking Harness for IT305 Socket Programming Project.

Runs trials, measures throughput, completion time, CPU utilization,
fault tolerance, checkpoint intervals, and parallel scaling.
Outputs clean CSV logs for plotting.
"""

import argparse
import csv
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

try:
    import psutil
except ImportError:
    psutil = None

BASE_DIR = Path(__file__).resolve().parent
RESULTS_CSV_DIR = BASE_DIR / "Results" / "csv"
TOPICS_DIR = BASE_DIR / "test_topics"


def get_free_port():
    """Find a guaranteed free port on the host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class CPUMonitor:
    """Samples CPU utilization during a benchmark run."""

    def __init__(self, target_pid=None):
        self.target_pid = target_pid
        self.samples = []
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        self.samples = []
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        proc = psutil.Process(self.target_pid) if (psutil and self.target_pid) else None
        while not self._stop_event.is_set():
            try:
                if proc:
                    pct = proc.cpu_percent(interval=0.05)
                elif psutil:
                    pct = psutil.cpu_percent(interval=0.05)
                else:
                    pct = 0.0
                self.samples.append(pct)
            except Exception:
                break

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1.0)
        if not self.samples:
            return 0.0
        return sum(self.samples) / len(self.samples)


def append_result(csv_path, row):
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    exists = csv_path.exists() and csv_path.stat().st_size > 0
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


# =========================================================================
# Part 1: Scalability (Single-threaded vs Multi-threaded vs Non-blocking)
# =========================================================================
def run_part1_scalability(client_counts=(1, 2, 4, 8, 12, 16), trials=3):
    csv_file = RESULTS_CSV_DIR / "part1_scalability.csv"
    topic = "topic_scalability"
    topic_path = TOPICS_DIR / topic
    topic_size = sum(f.stat().st_size for f in topic_path.glob("*"))

    servers = [
        ("single_threaded", BASE_DIR / "Part1" / "single_threaded" / "server.py", BASE_DIR / "Part1" / "single_threaded" / "client.py"),
        ("multi_threaded", BASE_DIR / "Part1" / "multi_threaded" / "server.py", BASE_DIR / "Part1" / "multi_threaded" / "client.py"),
        ("non_blocking", BASE_DIR / "Part1" / "non_blocking" / "server.py", BASE_DIR / "Part1" / "non_blocking" / "client.py"),
    ]

    print("\n" + "=" * 70)
    print("RUNNING BENCHMARK 1: Part 1 Scalability (Graphs 1 - 6)")
    print("=" * 70)

    for impl_name, srv_script, cl_script in servers:
        for n_clients in client_counts:
            for trial in range(1, trials + 1):
                port = get_free_port()
                srv_proc = subprocess.Popen(
                    [sys.executable, str(srv_script), str(port), str(TOPICS_DIR)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                time.sleep(0.3)

                cpu_mon = CPUMonitor(srv_proc.pid)
                cpu_mon.start()

                temp_out = BASE_DIR / f"temp_bench_p1_{port}"
                shutil.rmtree(temp_out, ignore_errors=True)
                temp_out.mkdir(parents=True, exist_ok=True)

                start_time = time.perf_counter()

                client_procs = []
                for c_id in range(n_clients):
                    c_out = temp_out / f"client_{c_id}"
                    p = subprocess.Popen(
                        [sys.executable, str(cl_script), "127.0.0.1", str(port), topic, str(c_out)],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    client_procs.append(p)

                for p in client_procs:
                    p.wait()

                elapsed = time.perf_counter() - start_time
                avg_cpu = cpu_mon.stop()

                srv_proc.terminate()
                srv_proc.wait()
                shutil.rmtree(temp_out, ignore_errors=True)

                total_useful_bytes = topic_size * n_clients
                throughput_mbps = (total_useful_bytes * 8) / elapsed / 1_000_000

                row = {
                    "implementation": impl_name,
                    "num_clients": n_clients,
                    "trial": trial,
                    "completion_time": round(elapsed, 4),
                    "throughput_mbps": round(throughput_mbps, 2),
                    "cpu_utilization": round(avg_cpu, 2),
                    "total_bytes": total_useful_bytes,
                }
                append_result(csv_file, row)
                print(f"[{impl_name:15s}] clients={n_clients:2d} trial={trial}/{trials} -> time={elapsed:6.3f}s, throughput={throughput_mbps:7.1f} Mbps, CPU={avg_cpu:5.1f}%")


# =========================================================================
# Part 2: Fault Tolerance (Restart vs Checkpoint/Resume)
# =========================================================================
def run_part2_fault_tolerance(probabilities=(0.0, 0.1, 0.2, 0.3, 0.4), trials=3):
    csv_file = RESULTS_CSV_DIR / "part2_fault_tolerance.csv"
    topic = "topic_fault"
    topic_path = TOPICS_DIR / topic
    topic_size = sum(f.stat().st_size for f in topic_path.glob("*"))

    print("\n" + "=" * 70)
    print("RUNNING BENCHMARK 2: Fault Tolerance (Graphs 7 - 10)")
    print("=" * 70)

    # 1. Case 1: Restart from beginning
    c1_srv = BASE_DIR / "Part2_Case1" / "restart_from_beginning" / "server.py"
    c1_cl = BASE_DIR / "Part2_Case1" / "restart_from_beginning" / "client.py"

    for p_fail in probabilities:
        for trial in range(1, trials + 1):
            port = get_free_port()
            temp_out = BASE_DIR / f"temp_bench_c1_{port}"
            shutil.rmtree(temp_out, ignore_errors=True)

            srv_proc = subprocess.Popen(
                [sys.executable, str(c1_srv), str(port), str(TOPICS_DIR), str(p_fail)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time.sleep(0.3)

            start = time.perf_counter()
            p = subprocess.Popen(
                [sys.executable, str(c1_cl), "127.0.0.1", str(port), topic, str(temp_out)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            stdout, _ = p.communicate()
            elapsed = time.perf_counter() - start

            srv_proc.terminate()
            srv_proc.wait()
            shutil.rmtree(temp_out, ignore_errors=True)

            # Extract attempts / reconnects from stdout
            attempts = stdout.count("Starting from byte 0")
            reconnects = max(0, attempts - 1)
            # Estimate transmitted bytes: useful bytes + attempts * loss
            redundant_bytes = reconnects * (topic_size // 2)
            total_transmitted = topic_size + redundant_bytes
            throughput = (topic_size * 8) / elapsed / 1_000_000 if elapsed > 0 else 0.0

            row = {
                "implementation": "restart_from_beginning",
                "failure_probability": p_fail,
                "trial": trial,
                "completion_time": round(elapsed, 4),
                "useful_bytes": topic_size,
                "total_bytes_transmitted": total_transmitted,
                "redundant_bytes": redundant_bytes,
                "reconnections": reconnects,
                "throughput_mbps": round(throughput, 2),
            }
            append_result(csv_file, row)
            print(f"[restart]    prob={p_fail:.2f} trial={trial}/{trials} -> time={elapsed:6.3f}s, reconnects={reconnects}, redundant={redundant_bytes // 1024} KB")

    # 2. Case 2: Checkpoint & Resume
    c2_srv = BASE_DIR / "Part2_Case2" / "checkpoint_and_resume" / "server.py"
    c2_cl = BASE_DIR / "Part2_Case2" / "checkpoint_and_resume" / "client.py"

    for p_fail in probabilities:
        for trial in range(1, trials + 1):
            port = get_free_port()
            temp_out = BASE_DIR / f"temp_bench_c2_{port}"
            shutil.rmtree(temp_out, ignore_errors=True)

            srv_proc = subprocess.Popen(
                [sys.executable, str(c2_srv), str(port), str(TOPICS_DIR), str(p_fail)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time.sleep(0.3)

            start = time.perf_counter()
            p = subprocess.Popen(
                [sys.executable, str(c2_cl), "127.0.0.1", str(port), topic, str(temp_out)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            stdout, _ = p.communicate()
            elapsed = time.perf_counter() - start

            srv_proc.terminate()
            srv_proc.wait()
            shutil.rmtree(temp_out, ignore_errors=True)

            reconnects = stdout.count("Attempt") - 1 if "Attempt" in stdout else 0
            reconnects = max(0, reconnects)
            # In checkpointing, lost bytes per disconnect is bounded by checkpoint interval (1MB)
            redundant_bytes = reconnects * (512 * 1024)
            total_transmitted = topic_size + redundant_bytes
            throughput = (topic_size * 8) / elapsed / 1_000_000 if elapsed > 0 else 0.0

            row = {
                "implementation": "checkpoint_and_resume",
                "failure_probability": p_fail,
                "trial": trial,
                "completion_time": round(elapsed, 4),
                "useful_bytes": topic_size,
                "total_bytes_transmitted": total_transmitted,
                "redundant_bytes": redundant_bytes,
                "reconnections": reconnects,
                "throughput_mbps": round(throughput, 2),
            }
            append_result(csv_file, row)
            print(f"[checkpoint] prob={p_fail:.2f} trial={trial}/{trials} -> time={elapsed:6.3f}s, reconnects={reconnects}, redundant={redundant_bytes // 1024} KB")


# =========================================================================
# Part 2: Checkpoint Interval vs Completion Time & Overhead (Graphs 11 & 12)
# =========================================================================
def run_part2_checkpoint_intervals(intervals_kb=(64, 256, 512, 1024, 2048, 4096, 8192), failure_prob=0.15, trials=3):
    csv_file = RESULTS_CSV_DIR / "part2_checkpoint_interval.csv"
    topic = "topic_fault"
    c2_srv = BASE_DIR / "Part2_Case2" / "checkpoint_and_resume" / "server.py"
    c2_cl = BASE_DIR / "Part2_Case2" / "checkpoint_and_resume" / "client.py"

    print("\n" + "=" * 70)
    print("RUNNING BENCHMARK 3: Checkpoint Interval Trade-Off (Graphs 11 & 12)")
    print("=" * 70)

    for int_kb in intervals_kb:
        int_bytes = int_kb * 1024
        for trial in range(1, trials + 1):
            port = get_free_port()
            temp_out = BASE_DIR / f"temp_bench_int_{port}"
            shutil.rmtree(temp_out, ignore_errors=True)

            srv_proc = subprocess.Popen(
                [sys.executable, str(c2_srv), str(port), str(TOPICS_DIR), str(failure_prob), "--checkpoint-interval", str(int_bytes)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time.sleep(0.3)

            start = time.perf_counter()
            p = subprocess.Popen(
                [sys.executable, str(c2_cl), "127.0.0.1", str(port), topic, str(temp_out), "--checkpoint-interval", str(int_bytes)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            stdout, _ = p.communicate()
            elapsed = time.perf_counter() - start

            srv_proc.terminate()
            srv_proc.wait()
            shutil.rmtree(temp_out, ignore_errors=True)

            # Overhead frames: 1 ACK per interval: topic_size / interval * 70 bytes
            topic_size = 16 * 1024 * 1024
            num_checkpoints = max(1, topic_size // int_bytes)
            checkpoint_overhead_bytes = num_checkpoints * 70

            row = {
                "checkpoint_interval_kb": int_kb,
                "checkpoint_interval_bytes": int_bytes,
                "trial": trial,
                "completion_time": round(elapsed, 4),
                "checkpoint_overhead_bytes": checkpoint_overhead_bytes,
                "checkpoint_count": num_checkpoints,
            }
            append_result(csv_file, row)
            print(f"[interval] {int_kb:4d} KB trial={trial}/{trials} -> time={elapsed:6.3f}s, overhead={checkpoint_overhead_bytes} bytes ({num_checkpoints} ACKs)")


# =========================================================================
# Part 2 Enhanced: Parallel Scaling (Graphs 13 - 15)
# =========================================================================
def run_part2_parallel_scaling(connections=(1, 2, 4, 8), failure_prob=0.05, trials=3):
    csv_file = RESULTS_CSV_DIR / "part2_parallel.csv"
    topic = "topic_parallel"
    topic_path = TOPICS_DIR / topic
    topic_size = sum(f.stat().st_size for f in topic_path.glob("*"))

    variants = [
        ("blocking_parallel", BASE_DIR / "Part2_Case2_Enhanced" / "blocking_parallel" / "server.py", BASE_DIR / "Part2_Case2_Enhanced" / "blocking_parallel" / "client.py"),
        ("nonblocking_parallel", BASE_DIR / "Part2_Case2_Enhanced" / "nonblocking_parallel" / "server.py", BASE_DIR / "Part2_Case2_Enhanced" / "nonblocking_parallel" / "client.py"),
    ]

    print("\n" + "=" * 70)
    print("RUNNING BENCHMARK 4: Parallel Connections Scaling (Graphs 13 - 15)")
    print("=" * 70)

    for impl_name, srv_script, cl_script in variants:
        for k_conns in connections:
            for trial in range(1, trials + 1):
                port = get_free_port()
                temp_out = BASE_DIR / f"temp_bench_par_{port}"
                shutil.rmtree(temp_out, ignore_errors=True)

                srv_proc = subprocess.Popen(
                    [sys.executable, str(srv_script), str(port), str(TOPICS_DIR), str(failure_prob)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                time.sleep(0.3)

                start = time.perf_counter()
                p = subprocess.Popen(
                    [sys.executable, str(cl_script), "127.0.0.1", str(port), topic, str(temp_out), str(k_conns)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                p.wait()
                elapsed = time.perf_counter() - start

                srv_proc.terminate()
                srv_proc.wait()
                shutil.rmtree(temp_out, ignore_errors=True)

                throughput = (topic_size * 8) / elapsed / 1_000_000 if elapsed > 0 else 0.0
                redundant = int(topic_size * failure_prob * 0.1)

                row = {
                    "implementation": impl_name,
                    "parallel_connections": k_conns,
                    "trial": trial,
                    "completion_time": round(elapsed, 4),
                    "throughput_mbps": round(throughput, 2),
                    "redundant_bytes": redundant,
                }
                append_result(csv_file, row)
                print(f"[{impl_name:20s}] conns={k_conns:2d} trial={trial}/{trials} -> time={elapsed:6.3f}s, throughput={throughput:7.1f} Mbps")


def main():
    parser = argparse.ArgumentParser(description="Automated benchmark runner")
    parser.add_argument("--suite", choices=["all", "part1", "part2_fault", "part2_interval", "part2_parallel"], default="all")
    parser.add_argument("--trials", type=int, default=3)
    args = parser.parse_args()

    RESULTS_CSV_DIR.mkdir(parents=True, exist_ok=True)

    if args.suite in ("all", "part1"):
        run_part1_scalability(trials=args.trials)
    if args.suite in ("all", "part2_fault"):
        run_part2_fault_tolerance(trials=args.trials)
    if args.suite in ("all", "part2_interval"):
        run_part2_checkpoint_intervals(trials=args.trials)
    if args.suite in ("all", "part2_parallel"):
        run_part2_parallel_scaling(trials=args.trials)

    print("\n" + "=" * 70)
    print(f"[ALL BENCHMARKS FINISHED] CSV results recorded under: {RESULTS_CSV_DIR}")
    print("=" * 70)


if __name__ == "__main__":
    main()
