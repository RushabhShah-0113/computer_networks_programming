import argparse
import os
import shutil
import socket
import time

from protocol import (
    GET, FILE_INFO, FILE_DATA, TRANSFER_COMPLETE, ERROR,
    send_frame, recv_frame, encode_json, decode_json, decode_file_data,
)
from metrics import TransferMetrics, append_csv


def safe_filename(name):
    return (
        isinstance(name, str)
        and name not in ("", ".", "..")
        and os.path.basename(name) == name
        and "/" not in name
        and "\\" not in name
    )


def receive_attempt(sock, topic, attempt_dir, metrics):
    """Receive a complete topic into a fresh attempt directory."""
    os.makedirs(attempt_dir, exist_ok=True)

    send_frame(sock, GET, encode_json({"topic": topic}))

    files_received = 0
    useful_bytes = 0

    while True:
        message_type, payload = recv_frame(sock)

        if message_type == ERROR:
            message = decode_json(payload)
            raise RuntimeError(message.get("message", "Server error"))

        if message_type == TRANSFER_COMPLETE:
            result = decode_json(payload)

            if result.get("topic") != topic:
                raise ValueError("Completed topic mismatch")
            if result.get("files_sent") != files_received:
                raise ValueError("File count mismatch")
            if result.get("total_bytes") != useful_bytes:
                raise ValueError("Topic byte count mismatch")

            return files_received, useful_bytes

        if message_type != FILE_INFO:
            raise ValueError(f"Unexpected frame type: {message_type}")

        info = decode_json(payload)
        filename = info.get("name")
        expected_size = info.get("size")

        if not safe_filename(filename):
            raise ValueError("Invalid filename in FILE_INFO")
        if (
            not isinstance(expected_size, int)
            or isinstance(expected_size, bool)
            or expected_size < 0
        ):
            raise ValueError("Invalid file size")

        path = os.path.join(attempt_dir, filename)
        received = 0

        with open(path, "wb") as file:
            while received < expected_size:
                data_type, data_payload = recv_frame(sock)

                if data_type == ERROR:
                    error = decode_json(data_payload)
                    raise RuntimeError(
                        error.get("message", "Transfer error")
                    )

                if data_type != FILE_DATA:
                    raise ValueError("Expected FILE_DATA")

                data_filename, chunk = decode_file_data(data_payload)

                if data_filename != filename:
                    raise ValueError("FILE_DATA filename mismatch")
                if not chunk:
                    raise ValueError("Empty chunk before file completion")
                if received + len(chunk) > expected_size:
                    raise ValueError("Received more bytes than expected")

                file.write(chunk)
                received += len(chunk)
                metrics.add_received_bytes(len(chunk))

        if received != expected_size:
            raise ValueError(f"Incomplete file: {filename}")

        files_received += 1
        useful_bytes += received
        print(f"[RECEIVED] {filename}: {received} bytes")


def download_topic(
    server_ip, server_port, topic, output_dir,
    max_attempts=10, retry_delay=0.5, timeout=300,
):
    metrics = TransferMetrics(topic=topic)
    metrics.start()  # Before the first GET request.

    os.makedirs(output_dir, exist_ok=True)

    final_dir = os.path.join(output_dir, topic)
    successful_attempt_dir = None
    result = None

    try:
        for attempt in range(1, max_attempts + 1):
            attempt_dir = os.path.join(
                output_dir, f".{topic}_attempt_{attempt}"
            )

            # Never retain data from a previous failed attempt.
            shutil.rmtree(attempt_dir, ignore_errors=True)
            os.makedirs(attempt_dir)

            try:
                print(f"[ATTEMPT {attempt}/{max_attempts}] Starting from byte 0")

                with socket.create_connection(
                    (server_ip, server_port), timeout=timeout
                ) as sock:
                    sock.settimeout(timeout)

                    if attempt > 1:
                        metrics.add_reconnect()

                    files_received, useful_bytes = receive_attempt(
                        sock, topic, attempt_dir, metrics
                    )

                successful_attempt_dir = attempt_dir
                metrics.useful_bytes = useful_bytes
                metrics.finish(success=True)

                result = {
                    "files_received": files_received,
                    "useful_bytes": useful_bytes,
                    "attempts_used": attempt,
                    "status": "success",
                }
                break

            except (
                OSError, ConnectionError, ValueError, RuntimeError
            ) as exc:
                metrics.add_failure()
                print(f"[FAILED] Attempt {attempt}: {exc}")

                # Delete all partial files: Case 1 never resumes.
                shutil.rmtree(attempt_dir, ignore_errors=True)

                if attempt < max_attempts:
                    print("[RETRY] Reconnecting; restarting the whole topic.")
                    time.sleep(retry_delay)

        if result is None:
            metrics.finish(success=False)
            result = {
                "files_received": 0,
                "useful_bytes": 0,
                "attempts_used": max_attempts,
                "status": "failed",
            }

        # Publish output only after the entire topic has completed.
        if result["status"] == "success":
            shutil.rmtree(final_dir, ignore_errors=True)
            os.replace(successful_attempt_dir, final_dir)

        # This is client-observed payload bytes, not an exact count of
        # bytes accepted by the server's sendall() calls.
        metrics.total_bytes_transmitted = metrics._received_payload_bytes

    finally:
        if metrics.end_time is None:
            metrics.finish(success=False)

    csv_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        "..", "..", "Results", "csv", "part2_case1_client.csv",
    ))

    append_csv(csv_path, {**metrics.summary(), **result})

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Case 1: restart the topic after a failure"
    )
    parser.add_argument("server_ip")
    parser.add_argument("server_port", type=int)
    parser.add_argument("topic")
    parser.add_argument("output_dir")
    parser.add_argument("--max-attempts", type=int, default=10)
    parser.add_argument("--retry-delay", type=float, default=0.5)
    parser.add_argument("--timeout", type=float, default=300)
    args = parser.parse_args()

    if not 1 <= args.server_port <= 65535:
        parser.error("Invalid server port")
    if args.max_attempts < 1:
        parser.error("--max-attempts must be at least 1")
    if args.retry_delay < 0 or args.timeout <= 0:
        parser.error("Invalid retry delay or timeout")
    if not safe_filename(args.topic):
        parser.error("Invalid topic name")

    result = download_topic(
        args.server_ip,
        args.server_port,
        args.topic,
        os.path.abspath(args.output_dir),
        args.max_attempts,
        args.retry_delay,
        args.timeout,
    )

    print(result)

    if result["status"] != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()