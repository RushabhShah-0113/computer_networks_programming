
import argparse
import csv
import os
import random
import socket
import threading
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
    send_json,
    recv_json,
    encode_file_data,
    encode_json,
    decode_json,
)
from session import SessionManager
from scheduler import FileScheduler
from metrics import TransferMetrics


CHECKPOINT_INTERVAL = 1024 * 1024
SOCKET_TIMEOUT = 60


class BlockingParallelServer:
    def __init__(
        self,
        port,
        topics_root_dir,
        failure_probability,
        metrics_csv="server_metrics.csv",
    ):
        self.port = port
        self.root = Path(topics_root_dir).resolve()
        self.failure_probability = failure_probability
        self.metrics_csv = metrics_csv

        self.sessions = SessionManager(str(self.root))
        self.scheduler = FileScheduler(self.sessions)

        self.server_socket = None
        self.shutdown_event = threading.Event()

        self.worker_counter = 0
        self.worker_counter_lock = threading.Lock()

        self.metrics_lock = threading.Lock()

    def next_worker_id(self):
        with self.worker_counter_lock:
            self.worker_counter += 1
            return self.worker_counter

    def log_metrics(self, record):
        """Append one connection record to the server CSV."""
        directory = os.path.dirname(self.metrics_csv)
        if directory:
            os.makedirs(directory, exist_ok=True)

        exists = (
            os.path.exists(self.metrics_csv)
            and os.path.getsize(self.metrics_csv) > 0
        )

        with self.metrics_lock:
            with open(
                self.metrics_csv,
                "a",
                newline="",
                encoding="utf-8",
            ) as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=list(record.keys()),
                )

                if not exists:
                    writer.writeheader()

                writer.writerow(record)

    def send_error(self, conn, message):
        try:
            send_json(conn, ERROR, {"error": str(message)})
        except (OSError, ConnectionError):
            pass

    def receive_initial_request(self, conn):
        """Read GET or RESUME_REQUEST and identify the topic."""
        message_type, payload = recv_frame(conn)

        if message_type not in (GET, RESUME_REQUEST):
            raise ProtocolError("Expected GET or RESUME_REQUEST")

        request = decode_json(payload)

        if not isinstance(request, dict):
            raise ProtocolError("Request must be a JSON object")

        topic = request.get("topic")

        if not isinstance(topic, str) or not topic:
            raise ProtocolError("Missing topic name")

        return message_type, topic, request

    def handle_resume_request(self, topic, request):
        """
        Validate a client-supplied checkpoint.

        The in-memory session manager remains authoritative while the
        server is running. A client checkpoint can reconstruct state
        after a server restart only when no worker owns that file.
        """
        filename = request.get("filename")
        offset = request.get("offset", 0)

        if filename is None:
            if offset != 0:
                return False, "Offset must be zero without a filename"

            return True, "Initial checkpoint"

        if not self.sessions.validate_resume(topic, filename, offset):
            return False, "Checkpoint failed source-file validation"

        try:
            state = self.sessions.get_file_state(topic, filename)
        except ValueError:
            return False, "Unknown file"

        if state["status"] == 1:
            return False, "File is already assigned to another worker"

        # A live server must not allow a client to advance the
        # server-confirmed offset beyond the existing ACK state.
        if offset > state["confirmed_offset"]:
            return False, "Checkpoint exceeds server-confirmed offset"

        if offset < state["confirmed_offset"]:
            # The client is behind. Resume from the server's confirmed
            # offset only if its local file can safely be truncated.
            offset = state["confirmed_offset"]

        self.sessions.reconstruct_checkpoint(topic, filename, offset)
        return True, "Checkpoint validated"

    def transfer_file(self, conn, topic, filename, worker_id, metrics):
        """
        Transfer one assigned file from its confirmed offset.

        The server advances the checkpoint only after receiving a
        matching CHECKPOINT_ACK.
        """
        source_path = (self.root / topic / filename).resolve()
        topic_dir = (self.root / topic).resolve()

        if source_path.parent != topic_dir or not source_path.is_file():
            raise ProtocolError("Invalid source file")

        state = self.sessions.get_file_state(topic, filename)
        file_size = state["size"]
        offset = state["confirmed_offset"]

        send_json(
            conn,
            FILE_INFO,
            {
                "name": filename,
                "size": file_size,
                "offset": offset,
            },
        )

        bytes_since_checkpoint = 0
        failure_threshold = None

        if random.random() < self.failure_probability and file_size > offset:
            failure_threshold = random.randint(1, file_size - offset)

        bytes_sent_this_file = 0
        next_checkpoint = min(
            offset + CHECKPOINT_INTERVAL,
            file_size,
        )

        with source_path.open("rb") as file:
            file.seek(offset)

            while offset < file_size:
                if self.shutdown_event.is_set():
                    raise ConnectionError("Server is shutting down")

                remaining = file_size - offset
                data = file.read(min(CHUNK_SIZE, remaining))

                if not data:
                    raise OSError("Unexpected end of source file")

                send_frame(
                    conn,
                    FILE_DATA,
                    encode_file_data(filename, data),
                )

                offset += len(data)
                bytes_sent_this_file += len(data)
                bytes_since_checkpoint += len(data)

                metrics.record_transmitted_bytes(len(data))

                # Simulate a dropped connection after sending file data
                # but before acknowledging an unconfirmed checkpoint.
                if (
                    failure_threshold is not None
                    and bytes_sent_this_file >= failure_threshold
                ):
                    metrics.record_failure()
                    raise ConnectionError(
                        "Injected connection failure"
                    )

                checkpoint_due = (
                    bytes_since_checkpoint >= CHECKPOINT_INTERVAL
                    or offset == file_size
                )

                if checkpoint_due:
                    checkpoint_started = time.perf_counter()

                    message_type, ack_payload = recv_frame(conn)
                    processing_seconds = (
                        time.perf_counter() - checkpoint_started
                    )

                    metrics.record_checkpoint_overhead(
                        HEADER_SIZE + len(ack_payload)
                    )

                    if message_type != CHECKPOINT_ACK:
                        raise ProtocolError(
                            "Expected CHECKPOINT_ACK"
                        )

                    ack = decode_json(ack_payload)

                    if not isinstance(ack, dict):
                        raise ProtocolError(
                            "Checkpoint ACK must be a JSON object"
                        )

                    if (
                        ack.get("topic") != topic
                        or ack.get("filename") != filename
                        or ack.get("offset") != offset
                    ):
                        raise ProtocolError(
                            "Checkpoint acknowledgement mismatch"
                        )

                    self.sessions.acknowledge_checkpoint(
                        topic,
                        filename,
                        offset,
                    )

                    metrics.record_checkpoint(processing_seconds)

                    bytes_since_checkpoint = 0
                    next_checkpoint = min(
                        offset + CHECKPOINT_INTERVAL,
                        file_size,
                    )

        # Zero-length files have no FILE_DATA chunks or ACKs.
        if file_size == 0:
            self.sessions.acknowledge_checkpoint(
                topic,
                filename,
                0,
            )

        self.scheduler.mark_completed(
            topic,
            filename,
            worker_id,
        )

        print(
            f"[Worker {worker_id}] Completed {filename} "
            f"({file_size} bytes)"
        )

    def worker(self, conn, address, worker_id):
        metrics = None
        topic = None
        active_file = None
        status = "error"
        started = time.perf_counter()

        try:
            conn.settimeout(SOCKET_TIMEOUT)

            message_type, topic, request = self.receive_initial_request(conn)

            filenames = self.scheduler.initialize_topic(topic)

            metrics = TransferMetrics(
                topic,
                implementation="blocking_parallel",
            )

            if message_type == RESUME_REQUEST:
                accepted, reason = self.handle_resume_request(
                    topic,
                    request,
                )

                send_json(
                    conn,
                    RESUME_RESPONSE,
                    {
                        "accepted": accepted,
                        "restart": not accepted,
                        "reason": reason,
                    },
                )

                if not accepted:
                    return

            else:
                send_json(
                    conn,
                    RESUME_RESPONSE,
                    {
                        "accepted": True,
                        "restart": False,
                        "reason": "Topic accepted",
                    },
                )

            print(
                f"[Worker {worker_id}] Connected: {address}, "
                f"topic={topic}, files={len(filenames)}"
            )

            while not self.shutdown_event.is_set():
                assignment = self.scheduler.get_next_file(
                    topic,
                    worker_id,
                )

                if assignment is None:
                    if self.scheduler.topic_completed(topic):
                        send_json(
                            conn,
                            TRANSFER_COMPLETE,
                            {
                                "topic": topic,
                                "files_sent": len(filenames),
                                "total_bytes": sum(
                                    self.sessions.get_file_state(
                                        topic,
                                        name,
                                    )["size"]
                                    for name in filenames
                                ),
                            },
                        )

                        status = "success"
                        break

                    # Other workers may still own the remaining files.
                    time.sleep(0.05)
                    continue

                active_file = assignment["filename"]

                try:
                    self.transfer_file(
                        conn,
                        topic,
                        active_file,
                        worker_id,
                        metrics,
                    )
                    active_file = None

                except Exception:
                    if active_file is not None:
                        self.scheduler.release_failed_file(
                            topic,
                            active_file,
                            worker_id,
                        )
                    raise

        except (OSError, ConnectionError, ProtocolError, ValueError) as exc:
            print(
                f"[Worker {worker_id}] Connection {address} failed: {exc}"
            )

            if active_file is not None and topic is not None:
                self.scheduler.release_failed_file(
                    topic,
                    active_file,
                    worker_id,
                )

        finally:
            conn.close()

            elapsed = time.perf_counter() - started

            record = {
                "timestamp": time.time(),
                "worker_id": worker_id,
                "client": str(address),
                "topic": topic or "",
                "status": status,
                "elapsed_seconds": round(elapsed, 6),
                "total_bytes_transmitted": (
                    metrics.total_bytes_transmitted if metrics else 0
                ),
                "checkpoint_overhead_bytes": (
                    metrics.checkpoint_overhead_bytes if metrics else 0
                ),
                "checkpoint_count": (
                    metrics.checkpoint_count if metrics else 0
                ),
                "checkpoint_processing_seconds": (
                    round(
                        metrics.checkpoint_processing_seconds,
                        6,
                    )
                    if metrics
                    else 0
                ),
                "failure_count": (
                    metrics.failure_count if metrics else 0
                ),
            }

            self.log_metrics(record)

    def serve_forever(self):
        with socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        ) as server:
            self.server_socket = server
            server.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_REUSEADDR,
                1,
            )
            server.bind(("0.0.0.0", self.port))
            server.listen()

            print(f"Blocking parallel server listening on {self.port}")
            print(f"Topics directory: {self.root}")
            print(f"Failure probability: {self.failure_probability}")

            try:
                while not self.shutdown_event.is_set():
                    conn, address = server.accept()
                    worker_id = self.next_worker_id()

                    thread = threading.Thread(
                        target=self.worker,
                        args=(conn, address, worker_id),
                        daemon=True,
                    )
                    thread.start()

            except KeyboardInterrupt:
                print("\nShutting down server...")
                self.shutdown_event.set()


def main():
    parser = argparse.ArgumentParser(
        description="Blocking parallel checkpoint/resume server"
    )
    parser.add_argument("port", type=int)
    parser.add_argument("topics_root_dir")
    parser.add_argument("failure_probability", type=float)
    parser.add_argument(
        "--metrics",
        default="server_metrics.csv",
    )

    args = parser.parse_args()

    if not 0 <= args.failure_probability <= 1:
        parser.error("failure_probability must be between 0 and 1")

    root = Path(args.topics_root_dir).resolve()

    if not root.is_dir():
        parser.error(f"Topics directory does not exist: {root}")

    server = BlockingParallelServer(
        args.port,
        str(root),
        args.failure_probability,
        args.metrics,
    )

    server.serve_forever()


if __name__ == "__main__":
    main()
