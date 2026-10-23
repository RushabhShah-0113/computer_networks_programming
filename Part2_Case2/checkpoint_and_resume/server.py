
import argparse
import csv
import os
import random
import socket
import struct
import time
from pathlib import Path

from protocol import (
    CHUNK_SIZE,
    HEADER_SIZE,
    GET,
    FILE_INFO,
    FILE_DATA,
    TRANSFER_COMPLETE,
    ERROR,
    CHECKPOINT_ACK,
    RESUME_REQUEST,
    RESUME_RESPONSE,
    ProtocolError,
    send_frame,
    recv_frame,
    encode_json,
    decode_json,
    encode_file_data,
)

CHECKPOINT_INTERVAL = 1024 * 1024  # Acknowledge every 1 MiB
SOCKET_TIMEOUT = 60

# In-memory confirmed offsets:
# {(topic, filename): confirmed_offset}
confirmed_offsets = {}


class InjectedFailure(ConnectionError):
    """Represents a deliberately injected connection failure."""


def safe_topic_directory(root, topic):
    """Resolve a topic directory while preventing path traversal."""
    if not topic or topic in {".", ".."}:
        return None

    if Path(topic).name != topic or "/" in topic or "\\" in topic:
        return None

    root_path = Path(root).resolve()
    topic_path = (root_path / topic).resolve()

    if topic_path.parent != root_path:
        return None

    if not topic_path.is_dir():
        return None

    return topic_path


def get_topic_files(topic_dir):
    """Return regular files in a stable, deterministic order."""
    return sorted(
        (
            path
            for path in topic_dir.iterdir()
            if path.is_file() and not path.is_symlink()
        ),
        key=lambda path: path.name,
    )


def send_json(sock, message_type, data):
    send_frame(sock, message_type, encode_json(data))


def send_error(sock, message):
    try:
        send_json(sock, ERROR, {"error": str(message)})
    except (OSError, ConnectionError):
        pass


def valid_filename(filename, file_map):
    return (
        isinstance(filename, str)
        and filename in file_map
        and Path(filename).name == filename
        and "/" not in filename
        and "\\" not in filename
    )


def validate_resume_request(request, topic, file_map):
    """
    Validate the client's checkpoint against the topic's actual files.

    completed_files contains files the client has fully received.
    filename and offset identify the current partial file.
    """
    if request.get("topic") != topic:
        return False, "Topic mismatch"

    completed = request.get("completed_files", [])
    filename = request.get("filename")
    offset = request.get("offset", 0)

    if not isinstance(completed, list):
        return False, "Invalid completed_files list"

    if not all(valid_filename(name, file_map) for name in completed):
        return False, "Invalid completed filename"

    if len(set(completed)) != len(completed):
        return False, "Duplicate completed filename"

    if filename is None:
        if offset != 0:
            return False, "Offset must be zero when filename is absent"
    else:
        if not valid_filename(filename, file_map):
            return False, "Invalid resume filename"

        if filename in completed:
            return False, "Current file cannot also be completed"

        if not isinstance(offset, int) or isinstance(offset, bool):
            return False, "Invalid resume offset"

        file_size = file_map[filename].stat().st_size

        if offset < 0 or offset > file_size:
            return False, "Resume offset is outside the file"

    # Completed files must precede the current file in canonical order.
    ordered_names = list(file_map)
    if filename is not None:
        current_index = ordered_names.index(filename)
        prior_names = set(ordered_names[:current_index])

        if not set(completed).issubset(prior_names):
            return False, "Completed files are inconsistent with file order"

    return True, ""


def handle_connection(
    conn,
    address,
    root,
    failure_probability,
    log_path,
    checkpoint_interval=CHECKPOINT_INTERVAL,
):
    started = time.perf_counter()
    topic = ""
    transmitted_payload_bytes = 0
    checkpoint_overhead_bytes = 0
    checkpoint_count = 0
    checkpoint_processing_seconds = 0.0
    injected_failure = False
    status = "error"

    try:
        conn.settimeout(SOCKET_TIMEOUT)

        message_type, payload = recv_frame(conn)

        if message_type == GET:
            request = decode_json(payload)
            topic = request.get("topic", "")

            topic_dir = safe_topic_directory(root, topic)
            if topic_dir is None:
                send_error(conn, "Unknown or invalid topic")
                return

            files = get_topic_files(topic_dir)
            file_map = {path.name: path for path in files}

            send_json(
                conn,
                RESUME_RESPONSE,
                {
                    "accepted": True,
                    "restart": True,
                    "filename": None,
                    "offset": 0,
                    "reason": "New transfer",
                },
            )

            completed_files = []
            current_filename = None
            current_offset = 0

        elif message_type == RESUME_REQUEST:
            request = decode_json(payload)
            topic = request.get("topic", "")

            topic_dir = safe_topic_directory(root, topic)
            if topic_dir is None:
                send_error(conn, "Unknown or invalid topic")
                return

            files = get_topic_files(topic_dir)
            file_map = {path.name: path for path in files}

            accepted, reason = validate_resume_request(
                request, topic, file_map
            )

            if not accepted:
                # Invalid checkpoints safely fall back to a fresh transfer.
                completed_files = []
                current_filename = None
                current_offset = 0

                send_json(
                    conn,
                    RESUME_RESPONSE,
                    {
                        "accepted": False,
                        "restart": True,
                        "filename": None,
                        "offset": 0,
                        "reason": reason,
                    },
                )
            else:
                completed_files = list(request.get("completed_files", []))
                current_filename = request.get("filename")
                current_offset = request.get("offset", 0)

                # Reconstruct lost in-memory state from the validated
                # checkpoint supplied by the client.
                if current_filename is not None:
                    confirmed_offsets[(topic, current_filename)] = (
                        current_offset
                    )

                send_json(
                    conn,
                    RESUME_RESPONSE,
                    {
                        "accepted": True,
                        "restart": False,
                        "filename": current_filename,
                        "offset": current_offset,
                        "reason": "Checkpoint validated",
                    },
                )

        else:
            send_error(conn, "Expected GET or RESUME_REQUEST")
            return

        ordered_names = [path.name for path in files]
        completed_set = set(completed_files)

        failure_after = None
        bytes_since_connection_start = 0

        # A failure is injected at a random payload-byte threshold.
        # The check occurs between chunks, so the failure is at a
        # chunk boundary rather than an arbitrary byte within a chunk.
        if random.random() < failure_probability:
            failure_after = random.randint(1, max(
                1,
                sum(
                    path.stat().st_size
                    for path in files
                    if path.name not in completed_set
                ),
            ))

        for filename in ordered_names:
            if filename in completed_set:
                continue

            path = file_map[filename]
            file_size = path.stat().st_size

            if filename == current_filename:
                offset = current_offset
            else:
                offset = 0

            # Announce file metadata and the starting offset.
            send_json(
                conn,
                FILE_INFO,
                {
                    "name": filename,
                    "size": file_size,
                    "offset": offset,
                },
            )

            confirmed_offset = offset
            sent_offset = offset
            unconfirmed_bytes = 0

            with path.open("rb") as file:
                file.seek(offset)

                while sent_offset < file_size:
                    # Stop before sending beyond the chosen failure
                    # threshold, when possible.
                    if (
                        failure_after is not None
                        and bytes_since_connection_start >= failure_after
                    ):
                        injected_failure = True
                        status = "injected_failure"
                        raise InjectedFailure(
                            "Injected disconnect during file transfer"
                        )

                    remaining = file_size - sent_offset
                    data = file.read(min(CHUNK_SIZE, remaining))

                    if not data:
                        raise OSError("Unexpected end of source file")

                    send_frame(
                        conn,
                        FILE_DATA,
                        encode_file_data(filename, data),
                    )

                    sent_offset += len(data)
                    unconfirmed_bytes += len(data)
                    bytes_since_connection_start += len(data)
                    transmitted_payload_bytes += len(data)

                    should_checkpoint = (
                        unconfirmed_bytes >= checkpoint_interval
                        or sent_offset == file_size
                    )

                    if (
                        failure_after is not None
                        and bytes_since_connection_start >= failure_after
                    ):
                        injected_failure = True
                        status = "injected_failure"
                        raise InjectedFailure(
                            "Injected disconnect during file transfer"
                        )

                    if should_checkpoint:
                        checkpoint_started = time.perf_counter()

                        ack_type, ack_payload = recv_frame(conn)
                        checkpoint_overhead_bytes += (
                            HEADER_SIZE + len(ack_payload)
                        )

                        if ack_type != CHECKPOINT_ACK:
                            raise ProtocolError(
                                "Expected CHECKPOINT_ACK"
                            )

                        ack = decode_json(ack_payload)

                        if (
                            ack.get("topic") != topic
                            or ack.get("filename") != filename
                            or ack.get("offset") != sent_offset
                        ):
                            raise ProtocolError(
                                "Checkpoint acknowledgement mismatch"
                            )

                        # Only now is this offset server-confirmed.
                        confirmed_offset = sent_offset
                        confirmed_offsets[(topic, filename)] = (
                            confirmed_offset
                        )

                        checkpoint_count += 1
                        unconfirmed_bytes = 0
                        checkpoint_processing_seconds += (
                            time.perf_counter() - checkpoint_started
                        )

            completed_set.add(filename)
            current_filename = None
            current_offset = 0

        send_json(
            conn,
            TRANSFER_COMPLETE,
            {
                "topic": topic,
                "files_sent": len(completed_set),
                "total_bytes": sum(
                    file_map[name].stat().st_size
                    for name in completed_set
                ),
            },
        )
        status = "success"

    except InjectedFailure:
        # Closing this connection simulates a network disconnect.
        pass

    except (ConnectionError, OSError, ProtocolError, ValueError, TypeError) as exc:
        print(f"[{address}] Transfer error: {exc}")

    finally:
        conn.close()

        elapsed = max(0.0, time.perf_counter() - started)

        print(
            f"[{address}] topic={topic!r} "
            f"status={status} "
            f"payload_bytes_sent={transmitted_payload_bytes} "
            f"checkpoint_count={checkpoint_count} "
            f"checkpoint_overhead_bytes={checkpoint_overhead_bytes} "
            f"checkpoint_processing_seconds="
            f"{checkpoint_processing_seconds:.6f} "
            f"injected_failure={injected_failure} "
            f"elapsed_seconds={elapsed:.6f}"
        )

        if log_path:
            write_server_log(
                log_path,
                {
                    "timestamp": time.time(),
                    "client": str(address),
                    "topic": topic,
                    "status": status,
                    "payload_bytes_sent": transmitted_payload_bytes,
                    "checkpoint_count": checkpoint_count,
                    "checkpoint_overhead_bytes": checkpoint_overhead_bytes,
                    "checkpoint_processing_seconds": (
                        checkpoint_processing_seconds
                    ),
                    "injected_failure": injected_failure,
                    "elapsed_seconds": elapsed,
                },
            )


def write_server_log(log_path, record):
    directory = os.path.dirname(log_path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    exists = os.path.exists(log_path) and os.path.getsize(log_path) > 0

    with open(log_path, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(record.keys()))

        if not exists:
            writer.writeheader()

        writer.writerow(record)


def main():
    parser = argparse.ArgumentParser(
        description="Checkpoint-and-resume file distribution server"
    )
    parser.add_argument("port", type=int)
    parser.add_argument("topics_root_dir")
    parser.add_argument("failure_probability", type=float)
    parser.add_argument(
        "--log",
        default="server_metrics.csv",
        help="CSV path for per-connection server metrics",
    )
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=CHECKPOINT_INTERVAL,
        help="Byte interval between checkpoints",
    )
    args = parser.parse_args()

    if not 0.0 <= args.failure_probability <= 1.0:
        parser.error("failure_probability must be between 0 and 1")

    root = os.path.abspath(args.topics_root_dir)
    if not os.path.isdir(root):
        parser.error(f"Topics directory does not exist: {root}")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", args.port))
        server.listen()

        print(f"Server listening on port {args.port}")
        print(f"Topics directory: {root}")
        print(f"Failure probability: {args.failure_probability}")
        print(f"Checkpoint interval: {args.checkpoint_interval} bytes")
        print(f"Server metrics CSV: {args.log}")

        while True:
            conn, address = server.accept()
            print(f"Connection from {address}")

            handle_connection(
                conn,
                address,
                root,
                args.failure_probability,
                args.log,
                checkpoint_interval=args.checkpoint_interval,
            )


if __name__ == "__main__":
    main()
