
"""Run multi-client experiments against the single-threaded server."""

import argparse
import csv
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor


def wait_for_server(host, port, timeout=10):
    """Wait until the server accepts TCP connections."""
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)

    raise TimeoutError(f"Server {host}:{port} did not become ready")


def run_trial(args, num_clients, trial):
    """Launch clients together and measure total completion time."""
    client_script = os.path.join(os.path.dirname(__file__), "client.py")
    commands = []

    for i in range(num_clients):
        output_dir = os.path.join(
            args.output_root, f"clients_{num_clients}",
            f"trial_{trial}", f"client_{i + 1}"
        )

        command = [
            sys.executable,
            client_script,
            args.host,
            str(args.port),
            args.topic,
            output_dir,
        ]
        commands.append(command)

    start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        processes = [
            executor.submit(
                subprocess.run,
                command,
                capture_output=True,
                text=True,
                timeout=args.client_timeout,
            )
            for command in commands
        ]
        results = [future.result() for future in processes]

    elapsed = time.perf_counter() - start
    successful = all(result.returncode == 0 for result in results)

    if not successful:
        for i, result in enumerate(results, start=1):
            if result.returncode != 0:
                print(f"[FAILED] Client {i}:\n{result.stderr}")
        status = "failed"
        throughput_mbps = 0.0
        useful_bytes = 0
    else:
        # All clients download the same topic. Count each delivered copy
        # as useful application payload for aggregate throughput.
        useful_bytes = sum(
            os.path.getsize(os.path.join(
                args.output_root, f"clients_{num_clients}",
                f"trial_{trial}", f"client_{i + 1}",
                args.topic, name
            ))
            for i in range(num_clients)
            for name in os.listdir(os.path.join(
                args.output_root, f"clients_{num_clients}",
                f"trial_{trial}", f"client_{i + 1}", args.topic
            ))
            if os.path.isfile(os.path.join(
                args.output_root, f"clients_{num_clients}",
                f"trial_{trial}", f"client_{i + 1}",
                args.topic, name
            ))
        )
        throughput_mbps = useful_bytes * 8 / elapsed / 1_000_000
        status = "success"

    row = {
        "num_clients": num_clients,
        "trial": trial,
        "topic": args.topic,
        "elapsed_seconds": round(elapsed, 6),
        "useful_bytes": useful_bytes,
        "aggregate_throughput_mbps": round(throughput_mbps, 6),
        "status": status,
    }

    print(row)
    return row


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--topic", default="cane")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--max-clients", type=int, default=6)
    parser.add_argument("--client-timeout", type=int, default=600)
    parser.add_argument("--output-root", default="../../experiment_downloads")
    parser.add_argument(
        "--csv",
        default="../../Results/csv/part1_single_threaded_experiments.csv",
    )
    args = parser.parse_args()

    try:
        wait_for_server(args.host, args.port)
    except TimeoutError as exc:
        parser.error(str(exc))

    rows = []
    for num_clients in range(1, args.max_clients + 1):
        for trial in range(1, args.trials + 1):
            rows.append(run_trial(args, num_clients, trial))

    csv_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), args.csv)
    )
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    with open(csv_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nExperiment results saved to: {csv_path}")


if __name__ == "__main__":
    main()
