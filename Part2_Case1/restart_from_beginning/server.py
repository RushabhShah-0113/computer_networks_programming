import argparse
import csv
import os
import random
import socket
import threading
import time
import uuid

from protocol import (
    GET, FILE_INFO, FILE_DATA, TRANSFER_COMPLETE, ERROR,
    CHUNK_SIZE, send_frame, recv_frame, encode_json, decode_json,
    encode_file_data,
)

log_lock = threading.Lock()


def log_attempt(path, row):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with log_lock:
        exists = os.path.exists(path) and os.path.getsize(path) > 0
        with open(path, "a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=list(row.keys()))
            if not exists:
                writer.writeheader()
            writer.writerow(row)


def get_topic_files(root_dir, topic):
    if (
        not isinstance(topic, str) or not topic
        or topic in (".", "..")
        or os.path.basename(topic) != topic
        or "/" in topic or "\\" in topic
    ):
        raise ValueError("Invalid topic name")

    root = os.path.realpath(root_dir)
    folder = os.path.realpath(os.path.join(root, topic))

    if os.path.commonpath([root, folder]) != root:
        raise ValueError("Invalid topic path")
    if not os.path.isdir(folder):
        raise FileNotFoundError(f"Topic not found: {topic}")

    files = []
    for name in sorted(os.listdir(folder)):
        path = os.path.join(folder, name)
        if os.path.islink(path) or not os.path.isfile(path):
            continue
        files.append({
            "name": name,
            "size": os.path.getsize(path),
            "path": path,
        })

    return files


def handle_client(conn, address, root_dir, failure_probability, log_path):
    attempt_id = uuid.uuid4().hex
    topic = ""
    payload_bytes_sent = 0
    failure_injected = 0
    status = "error"
    start = time.perf_counter()

    try:
        message_type, payload = recv_frame(conn)
        if message_type != GET:
            send_frame(conn, ERROR, encode_json({"message": "Expected GET"}))
            return

        request = decode_json(payload)
        topic = request.get("topic")
        files = get_topic_files(root_dir, topic)

        # One probabilistic decision for this connection's active transfer.
        total_size = sum(item["size"] for item in files)
        inject_failure = total_size > 0 and random.random() < failure_probability
        failure_after = random.randint(1, total_size) if inject_failure else None

        bytes_this_attempt = 0

        for item in files:
            name = item["name"]
            size = item["size"]

            send_frame(
                conn, FILE_INFO,
                encode_json({"name": name, "size": size}),
            )

            file_bytes_sent = 0
            with open(item["path"], "rb") as file:
                while True:
                    chunk = file.read(CHUNK_SIZE)
                    if not chunk:
                        break

                    frame_payload = encode_file_data(name, chunk)
                    send_frame(conn, FILE_DATA, frame_payload)

                    count = len(chunk)
                    file_bytes_sent += count
                    bytes_this_attempt += count
                    payload_bytes_sent += count

                    if (
                        inject_failure
                        and bytes_this_attempt >= failure_after
                    ):
                        failure_injected = 1
                        status = "injected_failure"
                        print(
                            f"[INJECTED FAILURE] {address}, topic={topic}, "
                            f"payload_bytes_sent={payload_bytes_sent}"
                        )
                        return

            if file_bytes_sent != size:
                raise OSError(f"File changed during transfer: {name}")

        send_frame(
            conn, TRANSFER_COMPLETE,
            encode_json({
                "topic": topic,
                "files_sent": len(files),
                "total_bytes": total_size,
            }),
        )
        status = "success"
        print(
            f"[COMPLETE] {address}, topic={topic}, "
            f"payload_bytes_sent={payload_bytes_sent}"
        )

    except (ConnectionError, OSError, ValueError, KeyError) as exc:
        print(f"[SERVER ERROR] {address}: {exc}")
        try:
            send_frame(conn, ERROR, encode_json({"message": str(exc)}))
        except OSError:
            pass

    finally:
        conn.close()
        log_attempt(log_path, {
            "attempt_id": attempt_id,
            "topic": topic,
            "client": str(address),
            "payload_bytes_sent": payload_bytes_sent,
            "failure_injected": failure_injected,
            "status": status,
            "elapsed_seconds": round(time.perf_counter() - start, 6),
        })


def main():
    parser = argparse.ArgumentParser(description="Case 1 restart server")
    parser.add_argument("port", type=int)
    parser.add_argument("topics_root_dir")
    parser.add_argument("failure_probability", type=float)
    parser.add_argument(
        "--log",
        default=os.path.join(
            os.path.dirname(__file__),
            "..", "..", "Results", "csv", "case1_server_attempts.csv",
        ),
    )
    args = parser.parse_args()

    if not 1 <= args.port <= 65535:
        parser.error("Port must be between 1 and 65535")
    if not 0.0 <= args.failure_probability <= 1.0:
        parser.error("Failure probability must be between 0.0 and 1.0")

    root_dir = os.path.abspath(args.topics_root_dir)
    if not os.path.isdir(root_dir):
        parser.error(f"Topics directory does not exist: {root_dir}")

    log_path = os.path.abspath(args.log)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", args.port))
    server.listen()

    print(f"[LISTENING] 0.0.0.0:{args.port}")
    print(f"[FAILURE PROBABILITY] {args.failure_probability}")
    print(f"[SERVER LOG] {log_path}")

    try:
        while True:
            conn, address = server.accept()
            # Case 1 is intentionally single-threaded here.
            handle_client(
                conn, address, root_dir,
                args.failure_probability, log_path,
            )
    except KeyboardInterrupt:
        print("\n[SHUTDOWN]")
    finally:
        server.close()


if __name__ == "__main__":
    main()