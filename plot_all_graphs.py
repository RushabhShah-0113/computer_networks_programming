"""Automated Graph Generation Engine for IT305 Socket Programming Evaluation.

Reads CSV logs from Results/csv/ and generates all 15 publication-grade graphs
in Results/graphs/ matching the lab assignment requirements.
"""

import csv
from collections import defaultdict
from pathlib import Path
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent
CSV_DIR = BASE_DIR / "Results" / "csv"
GRAPHS_DIR = BASE_DIR / "Results" / "graphs"
GRAPHS_DIR.mkdir(parents=True, exist_ok=True)

# Styling configuration
plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
plt.rcParams.update({
    "font.size": 11,
    "axes.labelsize": 12,
    "axes.titlesize": 13,
    "legend.fontsize": 11,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "figure.titlesize": 14,
    "lines.linewidth": 2.2,
    "lines.markersize": 7,
})


def load_csv(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


# =========================================================================
# Part A: Server Performance and Scalability (Graphs 1 to 6)
# =========================================================================
def plot_part_a():
    csv_file = CSV_DIR / "part1_scalability.csv"
    if not csv_file.exists():
        print(f"Skipping Part A: {csv_file} does not exist")
        return

    rows = load_csv(csv_file)
    data = defaultdict(lambda: defaultdict(list))
    # data[impl][metric][n_clients] = [values]
    for r in rows:
        impl = r["implementation"]
        n = int(r["num_clients"])
        data[impl]["clients"].append(n)
        data[impl]["throughput"].append(float(r["throughput_mbps"]))
        data[impl]["time"].append(float(r["completion_time"]))
        data[impl]["cpu"].append(float(r["cpu_utilization"]))

    # Aggregator helper
    def get_series(impl, key):
        clients = sorted(set(data[impl]["clients"]))
        avg_vals = []
        for c in clients:
            vals = [v for ci, v in zip(data[impl]["clients"], data[impl][key]) if ci == c]
            avg_vals.append(sum(vals) / len(vals))
        return clients, avg_vals

    colors = {
        "single_threaded": "#e74c3c",
        "multi_threaded": "#2980b9",
        "non_blocking": "#27ae60",
    }
    labels = {
        "single_threaded": "Single-Threaded",
        "multi_threaded": "Multi-Threaded",
        "non_blocking": "Non-Blocking (select)",
    }
    markers = {
        "single_threaded": "o",
        "multi_threaded": "s",
        "non_blocking": "^",
    }

    # Graph 1: Single-threaded Throughput
    fig, ax = plt.subplots(figsize=(7, 4.5))
    x, y = get_series("single_threaded", "throughput")
    ax.plot(x, y, color=colors["single_threaded"], marker=markers["single_threaded"], label=labels["single_threaded"])
    ax.set_title("Graph 1: Number of Clients vs. Throughput (Single-Threaded)")
    ax.set_xlabel("Number of Concurrent Clients")
    ax.set_ylabel("Throughput (Mbps)")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend()
    fig.tight_layout()
    fig.savefig(GRAPHS_DIR / "graph_01_clients_vs_throughput_single.png", dpi=300)
    plt.close(fig)

    # Graph 2: Multi-threaded Throughput
    fig, ax = plt.subplots(figsize=(7, 4.5))
    x, y = get_series("multi_threaded", "throughput")
    ax.plot(x, y, color=colors["multi_threaded"], marker=markers["multi_threaded"], label=labels["multi_threaded"])
    ax.set_title("Graph 2: Number of Clients vs. Throughput (Multi-Threaded)")
    ax.set_xlabel("Number of Concurrent Clients")
    ax.set_ylabel("Throughput (Mbps)")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend()
    fig.tight_layout()
    fig.savefig(GRAPHS_DIR / "graph_02_clients_vs_throughput_multi.png", dpi=300)
    plt.close(fig)

    # Graph 3: Non-blocking Throughput
    fig, ax = plt.subplots(figsize=(7, 4.5))
    x, y = get_series("non_blocking", "throughput")
    ax.plot(x, y, color=colors["non_blocking"], marker=markers["non_blocking"], label=labels["non_blocking"])
    ax.set_title("Graph 3: Number of Clients vs. Throughput (Non-Blocking select)")
    ax.set_xlabel("Number of Concurrent Clients")
    ax.set_ylabel("Throughput (Mbps)")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend()
    fig.tight_layout()
    fig.savefig(GRAPHS_DIR / "graph_03_clients_vs_throughput_nonblocking.png", dpi=300)
    plt.close(fig)

    # Graph 4: Throughput Comparison (All Three)
    fig, ax = plt.subplots(figsize=(8, 5))
    for impl in ("single_threaded", "multi_threaded", "non_blocking"):
        x, y = get_series(impl, "throughput")
        ax.plot(x, y, color=colors[impl], marker=markers[impl], label=labels[impl])
    ax.set_title("Graph 4: Number of Clients vs. Throughput (Comparison of All Three)")
    ax.set_xlabel("Number of Concurrent Clients")
    ax.set_ylabel("Throughput (Mbps)")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend()
    fig.tight_layout()
    fig.savefig(GRAPHS_DIR / "graph_04_clients_vs_throughput_comparison.png", dpi=300)
    plt.close(fig)

    # Graph 5: Completion Time Comparison (All Three)
    fig, ax = plt.subplots(figsize=(8, 5))
    for impl in ("single_threaded", "multi_threaded", "non_blocking"):
        x, y = get_series(impl, "time")
        ax.plot(x, y, color=colors[impl], marker=markers[impl], label=labels[impl])
    ax.set_title("Graph 5: Number of Clients vs. Completion Time (Comparison of All Three)")
    ax.set_xlabel("Number of Concurrent Clients")
    ax.set_ylabel("Completion Time (seconds)")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend()
    fig.tight_layout()
    fig.savefig(GRAPHS_DIR / "graph_05_clients_vs_completion_time_comparison.png", dpi=300)
    plt.close(fig)

    # Graph 6: CPU Utilization Comparison (All Three)
    fig, ax = plt.subplots(figsize=(8, 5))
    for impl in ("single_threaded", "multi_threaded", "non_blocking"):
        x, y = get_series(impl, "cpu")
        ax.plot(x, y, color=colors[impl], marker=markers[impl], label=labels[impl])
    ax.set_title("Graph 6: Number of Clients vs. CPU Utilization (Comparison of All Three)")
    ax.set_xlabel("Number of Concurrent Clients")
    ax.set_ylabel("CPU Utilization (%)")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend()
    fig.tight_layout()
    fig.savefig(GRAPHS_DIR / "graph_06_clients_vs_cpu_utilization_comparison.png", dpi=300)
    plt.close(fig)

    print("[SUCCESS] Generated Graphs 1 - 6 in Results/graphs/")


# =========================================================================
# Part B: Fault Tolerance and Checkpointing (Graphs 7 to 12)
# =========================================================================
def plot_part_b():
    # 1. Fault tolerance CSV
    csv_fault = CSV_DIR / "part2_fault_tolerance.csv"
    if csv_fault.exists():
        rows = load_csv(csv_fault)
        data = defaultdict(lambda: defaultdict(list))
        for r in rows:
            impl = r["implementation"]
            p = float(r["failure_probability"])
            data[impl]["prob"].append(p)
            data[impl]["time"].append(float(r["completion_time"]))
            data[impl]["redundant_kb"].append(float(r["redundant_bytes"]) / 1024)
            data[impl]["reconnects"].append(float(r["reconnections"]))

        def get_series(impl, key):
            probs = sorted(set(data[impl]["prob"]))
            avg_vals = []
            for p in probs:
                vals = [v for pi, v in zip(data[impl]["prob"], data[impl][key]) if pi == p]
                avg_vals.append(sum(vals) / len(vals))
            return probs, avg_vals

        # Graph 7: Failure prob vs Completion time - Restart
        fig, ax = plt.subplots(figsize=(7, 4.5))
        x, y = get_series("restart_from_beginning", "time")
        ax.plot(x, y, color="#c0392b", marker="o", label="Restart from Beginning")
        ax.set_title("Graph 7: Failure Probability vs. Completion Time (Restart Approach)")
        ax.set_xlabel("Failure Probability")
        ax.set_ylabel("Completion Time (seconds)")
        ax.grid(True, linestyle="--", alpha=0.6)
        ax.legend()
        fig.tight_layout()
        fig.savefig(GRAPHS_DIR / "graph_07_failure_prob_vs_completion_time_restart.png", dpi=300)
        plt.close(fig)

        # Graph 8: Failure prob vs Completion time - Checkpoint/Resume
        fig, ax = plt.subplots(figsize=(7, 4.5))
        x, y = get_series("checkpoint_and_resume", "time")
        ax.plot(x, y, color="#2980b9", marker="s", label="Checkpoint / Resume")
        ax.set_title("Graph 8: Failure Probability vs. Completion Time (Checkpoint/Resume)")
        ax.set_xlabel("Failure Probability")
        ax.set_ylabel("Completion Time (seconds)")
        ax.grid(True, linestyle="--", alpha=0.6)
        ax.legend()
        fig.tight_layout()
        fig.savefig(GRAPHS_DIR / "graph_08_failure_prob_vs_completion_time_checkpoint.png", dpi=300)
        plt.close(fig)

        # Graph 9: Failure prob vs Redundant Bytes (Comparison)
        fig, ax = plt.subplots(figsize=(8, 5))
        x1, y1 = get_series("restart_from_beginning", "redundant_kb")
        x2, y2 = get_series("checkpoint_and_resume", "redundant_kb")
        ax.plot(x1, y1, color="#c0392b", marker="o", label="Restart from Beginning")
        ax.plot(x2, y2, color="#27ae60", marker="s", label="Checkpoint / Resume")
        ax.set_title("Graph 9: Failure Probability vs. Redundant Bytes Transmitted")
        ax.set_xlabel("Failure Probability")
        ax.set_ylabel("Redundant Bytes Transmitted (KB)")
        ax.grid(True, linestyle="--", alpha=0.6)
        ax.legend()
        fig.tight_layout()
        fig.savefig(GRAPHS_DIR / "graph_09_failure_prob_vs_redundant_bytes_comparison.png", dpi=300)
        plt.close(fig)

        # Graph 10: Failure prob vs Number of Reconnections (Checkpoint)
        fig, ax = plt.subplots(figsize=(7, 4.5))
        x, y = get_series("checkpoint_and_resume", "reconnects")
        ax.plot(x, y, color="#8e44ad", marker="D", label="Checkpoint / Resume")
        ax.set_title("Graph 10: Failure Probability vs. Number of Reconnections")
        ax.set_xlabel("Failure Probability")
        ax.set_ylabel("Number of Reconnections")
        ax.grid(True, linestyle="--", alpha=0.6)
        ax.legend()
        fig.tight_layout()
        fig.savefig(GRAPHS_DIR / "graph_10_failure_prob_vs_reconnections.png", dpi=300)
        plt.close(fig)

    # 2. Checkpoint interval CSV
    csv_interval = CSV_DIR / "part2_checkpoint_interval.csv"
    if csv_interval.exists():
        rows = load_csv(csv_interval)
        intervals = []
        times = []
        overheads = []
        for r in rows:
            intervals.append(int(r["checkpoint_interval_kb"]))
            times.append(float(r["completion_time"]))
            overheads.append(float(r["checkpoint_overhead_bytes"]))

        unique_ints = sorted(set(intervals))
        avg_times = [sum(t for i, t in zip(intervals, times) if i == ui) / intervals.count(ui) for ui in unique_ints]
        avg_overheads = [sum(o for i, o in zip(intervals, overheads) if i == ui) / intervals.count(ui) for ui in unique_ints]

        # Graph 11: Checkpoint Interval vs Completion Time
        fig, ax = plt.subplots(figsize=(7, 4.5))
        ax.plot(unique_ints, avg_times, color="#d35400", marker="o", label="Completion Time")
        ax.set_xscale("log", base=2)
        ax.set_title("Graph 11: Checkpoint Interval vs. Completion Time")
        ax.set_xlabel("Checkpoint Interval (KB, log scale)")
        ax.set_ylabel("Completion Time (seconds)")
        ax.grid(True, which="both", linestyle="--", alpha=0.6)
        ax.legend()
        fig.tight_layout()
        fig.savefig(GRAPHS_DIR / "graph_11_checkpoint_interval_vs_completion_time.png", dpi=300)
        plt.close(fig)

        # Graph 12: Checkpoint Interval vs Overhead
        fig, ax = plt.subplots(figsize=(7, 4.5))
        ax.plot(unique_ints, avg_overheads, color="#16a085", marker="s", label="Protocol Overhead")
        ax.set_xscale("log", base=2)
        ax.set_title("Graph 12: Checkpoint Interval vs. Checkpoint Overhead")
        ax.set_xlabel("Checkpoint Interval (KB, log scale)")
        ax.set_ylabel("Checkpoint Overhead (Bytes)")
        ax.grid(True, which="both", linestyle="--", alpha=0.6)
        ax.legend()
        fig.tight_layout()
        fig.savefig(GRAPHS_DIR / "graph_12_checkpoint_interval_vs_overhead.png", dpi=300)
        plt.close(fig)

    print("[SUCCESS] Generated Graphs 7 - 12 in Results/graphs/")


# =========================================================================
# Part C: Enhanced Parallel Implementation (Graphs 13 to 15)
# =========================================================================
def plot_part_c():
    csv_file = CSV_DIR / "part2_parallel.csv"
    if not csv_file.exists():
        print(f"Skipping Part C: {csv_file} does not exist")
        return

    rows = load_csv(csv_file)
    data = defaultdict(lambda: defaultdict(list))
    for r in rows:
        impl = r["implementation"]
        k = int(r["parallel_connections"])
        data[impl]["conns"].append(k)
        data[impl]["throughput"].append(float(r["throughput_mbps"]))
        data[impl]["time"].append(float(r["completion_time"]))
        data[impl]["redundant"].append(float(r["redundant_bytes"]))

    def get_series(impl, key):
        conns = sorted(set(data[impl]["conns"]))
        avg_vals = []
        for k in conns:
            vals = [v for ki, v in zip(data[impl]["conns"], data[impl][key]) if ki == k]
            avg_vals.append(sum(vals) / len(vals))
        return conns, avg_vals

    colors = {"blocking_parallel": "#2980b9", "nonblocking_parallel": "#27ae60"}
    labels = {"blocking_parallel": "Blocking Multi-Threaded", "nonblocking_parallel": "Non-Blocking (select)"}
    markers = {"blocking_parallel": "o", "nonblocking_parallel": "s"}

    # Graph 13: Parallel Conns vs Throughput
    fig, ax = plt.subplots(figsize=(8, 5))
    for impl in ("blocking_parallel", "nonblocking_parallel"):
        x, y = get_series(impl, "throughput")
        ax.plot(x, y, color=colors[impl], marker=markers[impl], label=labels[impl])
    ax.set_title("Graph 13: Parallel Connections vs. Throughput (Blocking vs. Non-Blocking)")
    ax.set_xlabel("Number of Parallel TCP Connections")
    ax.set_ylabel("Throughput (Mbps)")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend()
    fig.tight_layout()
    fig.savefig(GRAPHS_DIR / "graph_13_parallel_conns_vs_throughput.png", dpi=300)
    plt.close(fig)

    # Graph 14: Parallel Conns vs Completion Time
    fig, ax = plt.subplots(figsize=(8, 5))
    for impl in ("blocking_parallel", "nonblocking_parallel"):
        x, y = get_series(impl, "time")
        ax.plot(x, y, color=colors[impl], marker=markers[impl], label=labels[impl])
    ax.set_title("Graph 14: Parallel Connections vs. Completion Time (Blocking vs. Non-Blocking)")
    ax.set_xlabel("Number of Parallel TCP Connections")
    ax.set_ylabel("Completion Time (seconds)")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend()
    fig.tight_layout()
    fig.savefig(GRAPHS_DIR / "graph_14_parallel_conns_vs_completion_time.png", dpi=300)
    plt.close(fig)

    # Graph 15: Parallel Conns vs Redundant Bytes
    fig, ax = plt.subplots(figsize=(8, 5))
    for impl in ("blocking_parallel", "nonblocking_parallel"):
        x, y = get_series(impl, "redundant")
        ax.plot(x, y, color=colors[impl], marker=markers[impl], label=labels[impl])
    ax.set_title("Graph 15: Parallel Connections vs. Redundant Bytes (Blocking vs. Non-Blocking)")
    ax.set_xlabel("Number of Parallel TCP Connections")
    ax.set_ylabel("Redundant Bytes Transmitted")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend()
    fig.tight_layout()
    fig.savefig(GRAPHS_DIR / "graph_15_parallel_conns_vs_redundant_bytes.png", dpi=300)
    plt.close(fig)

    print("[SUCCESS] Generated Graphs 13 - 15 in Results/graphs/")


def main():
    print("=" * 70)
    print("GENERATING ALL 15 EVALUATION GRAPHS")
    print("=" * 70)
    plot_part_a()
    plot_part_b()
    plot_part_c()
    print("=" * 70)
    print(f"[ALL 15 GRAPHS READY] Saved in: {GRAPHS_DIR}")
    print("=" * 70)


if __name__ == "__main__":
    main()
