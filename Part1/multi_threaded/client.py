
"""Client for the single-threaded TCP topic-based file distribution service."""

import argparse
import os
import sys
import socket

from metrics import TransferMetrics, append_csv
from protocol import CHUNK_SIZE, recv_exact, recv_message, send_message


def safe_filename(name):
    """Ensure the server-provided filename cannot escape the output directory."""
    return (
        isinstance(name, str)
        and name not in ("", ".", "..")
        and os.path.basename(name) == name
        and "/" not in name
        and "\\" not in name
    )


def download_topic(server_ip, server_port, topic, output_dir):
    """Request a topic and download all its files."""
    metrics = TransferMetrics()
    total_files = 0

    with socket.create_connection(
        (server_ip, server_port), timeout=30
    ) as sock:
        sock.settimeout(300)

        # Request all files belonging to the selected topic.
        send_message(sock, {
            "type": "GET",
            "topic": topic,
        })

        response = recv_message(sock)

        if response.get("type") == "ERROR":
            raise RuntimeError(
                response.get("message", "The server rejected the request")
            )

        if response.get("type") != "TOPIC":
            raise RuntimeError("Unexpected response from server")

        if response.get("topic") != topic:
            raise RuntimeError("The server returned an unexpected topic")

        files = response.get("files")

        if not isinstance(files, list):
            raise RuntimeError("Invalid file manifest received")

        topic_output = os.path.join(output_dir, topic)
        os.makedirs(topic_output, exist_ok=True)

        seen_names = set()

        for item in files:
            if not isinstance(item, dict):
                raise RuntimeError("Invalid file entry in manifest")

            name = item.get("name")
            size = item.get("size")

            if not safe_filename(name):
                raise RuntimeError(f"Unsafe filename: {name!r}")

            if name in seen_names:
                raise RuntimeError(f"Duplicate filename: {name}")

            if type(size) is not int or size < 0:
                raise RuntimeError(f"Invalid file size for {name}")

            seen_names.add(name)
            destination = os.path.join(topic_output, name)
            temporary_path = destination + ".part"
            remaining = size

            print(f"[DOWNLOADING] {name} ({size:,} bytes)")

            try:
                # Stream the file to disk without loading it all into memory.
                with open(temporary_path, "wb") as target:
                    while remaining > 0:
                        chunk_size = min(CHUNK_SIZE, remaining)
                        chunk = recv_exact(sock, chunk_size)

                        target.write(chunk)
                        metrics.add_bytes(len(chunk))
                        remaining -= len(chunk)

                # Rename only after the entire file has been received.
                os.replace(temporary_path, destination)

            except Exception:
                if os.path.exists(temporary_path):
                    os.remove(temporary_path)
                raise

            total_files += 1
            print(f"[SAVED] {destination}")

        # Confirm the complete topic transfer.
        completion = recv_message(sock)

        if completion.get("type") != "TRANSFER_COMPLETE":
            raise RuntimeError("Server did not confirm transfer completion")

        if completion.get("files") != total_files:
            raise RuntimeError("File count does not match server confirmation")

        if completion.get("payload_bytes") != metrics.payload_bytes:
            raise RuntimeError("Payload byte count does not match")

    summary = metrics.summary()
    summary.update({
        "topic": topic,
        "server_ip": server_ip,
        "server_port": server_port,
        "files_downloaded": total_files,
        "status": "success",
    })

    # Save metrics in the repository's Results directory.
    repo_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )
    csv_path = os.path.join(
        repo_root, "Results", "csv", "part1_single_threaded.csv"
    )

    append_csv(csv_path, summary)

    print("\n========== TRANSFER SUMMARY ==========")
    print(f"Topic: {topic}")
    print(f"Files downloaded: {total_files}")
    print(f"Payload received: {metrics.payload_bytes:,} bytes")
    print(f"Elapsed time: {summary['elapsed_seconds']} seconds")
    print(f"Throughput: {summary['throughput_mbps']} Mbps")
    print(f"Metrics saved to: {csv_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Download all files belonging to a topic"
    )
    parser.add_argument("server_ip", help="Server IP address")
    parser.add_argument("server_port", type=int, help="Server TCP port")
    parser.add_argument("topic", help="Topic to download, e.g. cane")
    parser.add_argument("output_dir", help="Directory to store downloaded files")

    args = parser.parse_args()

    if not 1 <= args.server_port <= 65535:
        parser.error("Server port must be between 1 and 65535")

    try:
        download_topic(
            args.server_ip,
            args.server_port,
            args.topic,
            args.output_dir,
        )
    except (
        OSError,
        ConnectionError,
        RuntimeError,
        ValueError,
    ) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
