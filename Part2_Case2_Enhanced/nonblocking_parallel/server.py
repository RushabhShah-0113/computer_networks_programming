
"""Windows-compatible non-blocking parallel file distribution server.

Uses select.select() with a single event loop and non-blocking TCP sockets.
Protocol and scheduling are shared with the blocking implementation.
"""

import argparse
import csv
import random
import select
import socket
import time
from pathlib import Path

from protocol import (
    GET,
    FILE_INFO,
    FILE_DATA,
    TRANSFER_COMPLETE,
    ERROR,
    CHECKPOINT_ACK,
    RESUME_REQUEST,
    RESUME_RESPONSE,
    CHUNK_SIZE,
    MAX_PAYLOAD_SIZE,
    HEADER_SIZE,
    ProtocolError,
    encode_json_frame,
    encode_file_data,
    decode_json,
    extract_frames,
)
from session import SessionManager
from scheduler import FileScheduler


CHECKPOINT_INTERVAL = 1024 * 1024
READ_SIZE = 64 * 1024
SELECT_TIMEOUT = 0.1


class NonBlockingServer:
    def __init__(
        self,
        port,
        topics_root_dir,
        failure_probability=0.0,
        metrics_csv="server_metrics.csv",
    ):
        if not 0.0 <= failure_probability <= 1.0:
            raise ValueError("failure_probability must be between 0 and 1")

        self.port = port
        self.root = Path(topics_root_dir).resolve()
        self.failure_probability = failure_probability
        self.metrics_csv = Path(metrics_csv)

        if not self.root.is_dir():
            raise ValueError(f"Topics directory does not exist: {self.root}")

        self.sessions = SessionManager(self.root)
        self.scheduler = FileScheduler(self.sessions)

        self.server_socket = None
        self.running = True

        # Connections are indexed by file descriptor.
        self.connections = {}

        # select.select() operates on socket objects on Windows.
        self.read_sockets = set()
        self.write_sockets = set()

        self.metrics_rows = []

    # ---------------------------------------------------------
    # Socket setup and connection management
    # ---------------------------------------------------------

    def create_listener(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", self.port))
        server.listen()
        server.setblocking(False)

        self.server_socket = server
        self.read_sockets.add(server)

        print(f"Non-blocking server listening on port {self.port}")
        print(f"Topics root: {self.root}")
        print(f"Failure probability: {self.failure_probability}")

    def accept_connections(self):
        while True:
            try:
                conn, address = self.server_socket.accept()
            except BlockingIOError:
                return
            except OSError as exc:
                print(f"Accept error: {exc}")
                return

            conn.setblocking(False)
            fd = conn.fileno()

            state = {
                "socket": conn,
                "address": address,
                "input_buffer": bytearray(),
                "output_buffer": bytearray(),
                "output_kind": None,
                "output_chunk": None,
                "topic": None,
                "worker_id": f"{address[0]}:{address[1]}",
                "handshake_complete": False,
                "finish_after_write": False,
                "active_file": None,
                "file_handle": None,
                "file_size": 0,
                "file_offset": 0,
                "file_sent": 0,
                "confirmed_offset": 0,
                "awaiting_checkpoint": False,
                "pending_checkpoint": None,
                "failure_threshold": None,
                "failure_injected": False,
                "start_time": None,
                "bytes_transmitted": 0,
                "useful_bytes": 0,
                "checkpoint_overhead_bytes": 0,
                "checkpoint_count": 0,
                "checkpoint_processing_seconds": 0.0,
                "failure_count": 0,
                "reconnect_count": 0,
                "completed_files": set(),
                "metrics_written": False,
            }

            self.connections[fd] = state
            self.read_sockets.add(conn)

            print(f"Accepted connection from {address}")

    def close_connection(self, fd, reason="connection closed"):
        state = self.connections.pop(fd, None)
        if state is None:
            return

        conn = state["socket"]
        self.read_sockets.discard(conn)
        self.write_sockets.discard(conn)

        if state["file_handle"] is not None:
            try:
                state["file_handle"].close()
            except OSError:
                pass

        topic = state["topic"]
        filename = state["active_file"]

        if topic and filename:
            try:
                self.scheduler.release_failed_file(
                    topic, filename, state["worker_id"]
                )
            except (KeyError, ValueError) as exc:
                print(f"Could not release {filename}: {exc}")

        try:
            conn.close()
        except OSError:
            pass

        self.write_metrics_row(state)
        print(f"Closed {state['address']}: {reason}")

    # ---------------------------------------------------------
    # Output queue and non-blocking I/O
    # ---------------------------------------------------------

    def queue_bytes(self, state, data, kind=None, chunk=None):
        """Queue one complete frame for transmission."""
        if state["output_buffer"]:
            raise RuntimeError("Attempted to queue while output is pending")

        state["output_buffer"].extend(data)
        state["output_kind"] = kind
        state["output_chunk"] = chunk

        conn = state["socket"]
        self.write_sockets.add(conn)

    def queue_json(self, state, message_type, payload):
        self.queue_bytes(
            state,
            encode_json_frame(message_type, payload),
            kind="control",
        )

    def read_from_connection(self, fd, state):
        """Read all currently available bytes and parse complete frames."""
        conn = state["socket"]

        try:
            while fd in self.connections:
                data = conn.recv(READ_SIZE)

                if not data:
                    self.close_connection(fd, "peer disconnected")
                    return

                state["input_buffer"].extend(data)

                frames, remaining = extract_frames(state["input_buffer"])
                state["input_buffer"] = remaining

                if len(remaining) > MAX_PAYLOAD_SIZE + HEADER_SIZE:
                    raise ProtocolError("Input buffer exceeds frame limit")

                for message_type, payload in frames:
                    if fd not in self.connections:
                        return
                    self.handle_frame(fd, state, message_type, payload)

        except BlockingIOError:
            return
        except (
            OSError,
            ConnectionError,
            ProtocolError,
            ValueError,
            TypeError,
            KeyError,
        ) as exc:
            self.close_connection(fd, f"read/protocol error: {exc}")

    def write_to_connection(self, fd, state):
        """Send queued data without blocking the event loop."""
        conn = state["socket"]

        while fd in self.connections and state["output_buffer"]:
            try:
                sent = conn.send(state["output_buffer"])
            except BlockingIOError:
                return
            except OSError as exc:
                self.close_connection(fd, f"write error: {exc}")
                return

            if sent <= 0:
                self.close_connection(fd, "socket send returned zero")
                return

            del state["output_buffer"][:sent]

        if fd not in self.connections:
            return

        if state["output_buffer"]:
            return

        self.write_sockets.discard(conn)

        kind = state["output_kind"]
        chunk = state["output_chunk"]
        state["output_kind"] = None
        state["output_chunk"] = None

        if kind == "file_data" and chunk is not None:
            if not self.finish_chunk_send(fd, state, chunk):
                return

        if state["finish_after_write"]:
            self.close_connection(fd, "transfer completed")
            return

        # Generate the next frame only when the previous one is fully sent.
        self.prepare_next_output(fd, state)

    # ---------------------------------------------------------
    # Protocol and session handshake
    # ---------------------------------------------------------

    def handle_frame(self, fd, state, message_type, payload):
        if message_type in (GET, RESUME_REQUEST):
            self.handle_initial_request(state, message_type, payload)
        elif message_type == CHECKPOINT_ACK:
            self.handle_checkpoint_ack(state, payload)
        else:
            self.queue_json(
                state,
                ERROR,
                {"error": f"Unexpected message type: {message_type}"},
            )

    def handle_initial_request(self, state, message_type, payload):
        if state["topic"] is not None:
            self.queue_json(
                state,
                ERROR,
                {"error": "Initial request already received"},
            )
            return

        request = decode_json(payload)
        topic = request.get("topic")

        if not isinstance(topic, str) or not topic:
            self.queue_json(state, ERROR, {"error": "Invalid topic"})
            return

        try:
            self.scheduler.initialize_topic(topic)
        except (OSError, ValueError, KeyError) as exc:
            self.queue_json(state, ERROR, {"error": str(exc)})
            return

        state["topic"] = topic
        state["start_time"] = time.perf_counter()

        if message_type == RESUME_REQUEST:
            state["reconnect_count"] += 1
            self.validate_resume_request(state, request)

            # A validation response is already queued on failure.
            if state["output_buffer"]:
                return

        state["handshake_complete"] = True
        self.queue_json(
            state,
            RESUME_RESPONSE,
            {"accepted": True, "restart": False},
        )

    def validate_resume_request(self, state, request):
        filename = request.get("filename")
        offset = request.get("offset", 0)

        if filename is None:
            return

        if not isinstance(filename, str) or not isinstance(offset, int):
            self.queue_json(
                state,
                RESUME_RESPONSE,
                {
                    "accepted": False,
                    "restart": True,
                    "reason": "Invalid filename or offset",
                },
            )
            return

        file_state = self.sessions.get_file_state(state["topic"], filename)

        if file_state is None:
            self.queue_json(
                state,
                RESUME_RESPONSE,
                {
                    "accepted": False,
                    "restart": True,
                    "reason": "Unknown file",
                },
            )
            return

        confirmed = file_state["confirmed_offset"]

        if offset < 0 or offset > confirmed:
            self.queue_json(
                state,
                RESUME_RESPONSE,
                {
                    "accepted": False,
                    "restart": True,
                    "reason": "Offset exceeds server checkpoint",
                },
            )
            return

        if file_state["status"] == 1:
            self.queue_json(
                state,
                RESUME_RESPONSE,
                {
                    "accepted": False,
                    "restart": True,
                    "reason": "File is already assigned",
                },
            )
            return

        try:
            self.sessions.reconstruct_checkpoint(
                state["topic"], filename, confirmed
            )
        except (ValueError, KeyError) as exc:
            self.queue_json(
                state,
                RESUME_RESPONSE,
                {
                    "accepted": False,
                    "restart": True,
                    "reason": str(exc),
                },
            )

    # ---------------------------------------------------------
    # Dynamic file allocation and transmission
    # ---------------------------------------------------------

    def prepare_next_output(self, fd, state):
        if fd not in self.connections:
            return

        if not state["handshake_complete"]:
            return

        if state["output_buffer"] or state["awaiting_checkpoint"]:
            return

        if state["active_file"] is None:
            self.assign_next_file(fd, state)
            return

        if state["file_offset"] >= state["file_size"]:
            # A zero-byte file still requires an acknowledgement at offset 0.
            if state["pending_checkpoint"] is None:
                state["awaiting_checkpoint"] = True
                state["pending_checkpoint"] = state["file_offset"]
            return

        self.queue_next_chunk(state)

    def assign_next_file(self, fd, state):
        try:
            assignment = self.scheduler.get_next_file(
                state["topic"], state["worker_id"]
            )
        except (KeyError, ValueError, OSError) as exc:
            self.queue_json(state, ERROR, {"error": str(exc)})
            return

        if assignment is None:
            if self.scheduler.topic_completed(state["topic"]):
                self.queue_json(
                    state,
                    TRANSFER_COMPLETE,
                    {"topic": state["topic"]},
                )
                state["finish_after_write"] = True
            return

        filename = assignment["filename"]
        offset = assignment["offset"]
        size = assignment["size"]

        topic_dir = (self.root / state["topic"]).resolve()

        # Validate the path before opening it.
        file_path = (topic_dir / filename).resolve()

        if file_path.parent != topic_dir or not file_path.is_file():
            self.scheduler.release_failed_file(
                state["topic"], filename, state["worker_id"]
            )
            self.queue_json(
                state, ERROR, {"error": "Invalid source file path"}
            )
            return

        try:
            handle = open(file_path, "rb")
            handle.seek(offset)
        except OSError as exc:
            self.scheduler.release_failed_file(
                state["topic"], filename, state["worker_id"]
            )
            self.queue_json(
                state,
                ERROR,
                {"error": f"Cannot open {filename}: {exc}"},
            )
            return

        state["active_file"] = filename
        state["file_handle"] = handle
        state["file_size"] = size
        state["file_offset"] = offset
        state["confirmed_offset"] = offset
        state["file_sent"] = 0
        state["pending_checkpoint"] = None
        state["awaiting_checkpoint"] = False
        state["failure_injected"] = False

        remaining = size - offset

        if remaining > 0 and random.random() < self.failure_probability:
            state["failure_threshold"] = random.randint(1, remaining)
        else:
            state["failure_threshold"] = None

        self.queue_json(
            state,
            FILE_INFO,
            {
                "name": filename,
                "size": size,
                "offset": offset,
            },
        )

    def queue_next_chunk(self, state):
        filename = state["active_file"]
        chunk = state["file_handle"].read(CHUNK_SIZE)

        if not chunk:
            if state["file_offset"] == state["file_size"]:
                state["awaiting_checkpoint"] = True
                state["pending_checkpoint"] = state["file_offset"]
                return

            raise ConnectionError(f"Unexpected EOF in {filename}")

        frame = encode_file_data(filename, chunk)

        self.queue_bytes(
            state,
            frame,
            kind="file_data",
            chunk={
                "payload_bytes": len(chunk),
                "end_offset": state["file_offset"] + len(chunk),
            },
        )

    def finish_chunk_send(self, fd, state, chunk):
        payload_bytes = chunk["payload_bytes"]
        state["file_offset"] = chunk["end_offset"]
        state["file_sent"] += payload_bytes
        state["bytes_transmitted"] += payload_bytes

        threshold = state["failure_threshold"]

        if (
            threshold is not None
            and not state["failure_injected"]
            and state["file_sent"] >= threshold
        ):
            state["failure_injected"] = True
            state["failure_count"] += 1
            self.close_connection(fd, "injected transfer failure")
            return False

        unconfirmed = state["file_offset"] - state["confirmed_offset"]

        if (
            unconfirmed >= CHECKPOINT_INTERVAL
            or state["file_offset"] >= state["file_size"]
        ):
            state["awaiting_checkpoint"] = True
            state["pending_checkpoint"] = state["file_offset"]

        return True

    def handle_checkpoint_ack(self, state, payload):
        request = decode_json(payload)

        topic = request.get("topic")
        filename = request.get("filename")
        offset = request.get("offset")

        if (
            topic != state["topic"]
            or filename != state["active_file"]
            or not isinstance(offset, int)
            or offset != state["pending_checkpoint"]
            or not state["awaiting_checkpoint"]
        ):
            self.queue_json(
                state,
                ERROR,
                {"error": "Invalid checkpoint acknowledgement"},
            )
            return

        started = time.perf_counter()

        try:
            self.sessions.acknowledge_checkpoint(topic, filename, offset)
        except (ValueError, KeyError) as exc:
            self.queue_json(
                state,
                ERROR,
                {"error": f"Checkpoint rejected: {exc}"},
            )
            return

        state["checkpoint_processing_seconds"] += (
            time.perf_counter() - started
        )
        state["checkpoint_count"] += 1

        # Approximate application-level checkpoint overhead. The client and
        # server use the same JSON object fields, though wire framing adds
        # header bytes too.
        state["checkpoint_overhead_bytes"] += len(payload)

        state["confirmed_offset"] = offset
        state["pending_checkpoint"] = None
        state["awaiting_checkpoint"] = False

        if offset == state["file_size"]:
            self.complete_active_file(state)

    def complete_active_file(self, state):
        topic = state["topic"]
        filename = state["active_file"]

        if state["file_handle"] is not None:
            state["file_handle"].close()
            state["file_handle"] = None

        self.scheduler.mark_completed(
            topic, filename, state["worker_id"]
        )

        if filename not in state["completed_files"]:
            state["completed_files"].add(filename)
            state["useful_bytes"] += state["file_size"]

        state["active_file"] = None
        state["file_size"] = 0
        state["file_offset"] = 0
        state["file_sent"] = 0
        state["confirmed_offset"] = 0
        state["pending_checkpoint"] = None
        state["awaiting_checkpoint"] = False
        state["failure_threshold"] = None
        state["failure_injected"] = False

    # ---------------------------------------------------------
    # Metrics
    # ---------------------------------------------------------

    def write_metrics_row(self, state):
        if state["metrics_written"] or state["start_time"] is None:
            return

        state["metrics_written"] = True
        elapsed = max(time.perf_counter() - state["start_time"], 1e-9)

        transmitted = state["bytes_transmitted"]
        useful = state["useful_bytes"]

        self.metrics_rows.append(
            {
                "implementation": "nonblocking_select",
                "topic": state["topic"],
                "client": str(state["address"]),
                "elapsed_seconds": round(elapsed, 6),
                "total_bytes_transmitted": transmitted,
                "useful_bytes": useful,
                "redundant_bytes": max(transmitted - useful, 0),
                "throughput_mbps": round(
                    useful * 8 / elapsed / 1_000_000, 6
                ),
                "raw_payload_throughput_mbps": round(
                    transmitted * 8 / elapsed / 1_000_000, 6
                ),
                "checkpoint_overhead_bytes": state[
                    "checkpoint_overhead_bytes"
                ],
                "checkpoint_count": state["checkpoint_count"],
                "checkpoint_processing_seconds": round(
                    state["checkpoint_processing_seconds"], 6
                ),
                "failure_count": state["failure_count"],
                "reconnect_count": state["reconnect_count"],
            }
        )

    def save_metrics(self):
        if not self.metrics_rows:
            return

        self.metrics_csv.parent.mkdir(parents=True, exist_ok=True)
        exists = self.metrics_csv.exists() and self.metrics_csv.stat().st_size > 0

        fields = list(self.metrics_rows[0].keys())

        with self.metrics_csv.open(
            "a", newline="", encoding="utf-8"
        ) as file:
            writer = csv.DictWriter(file, fieldnames=fields)

            if not exists:
                writer.writeheader()

            writer.writerows(self.metrics_rows)

        self.metrics_rows.clear()

    # ---------------------------------------------------------
    # Event loop
    # ---------------------------------------------------------

    def run(self):
        self.create_listener()

        try:
            while self.running:
                read_list = list(self.read_sockets)
                write_list = list(self.write_sockets)
                exceptional_list = read_list + write_list

                try:
                    readable, writable, exceptional = select.select(
                        read_list,
                        write_list,
                        exceptional_list,
                        SELECT_TIMEOUT,
                    )
                except OSError as exc:
                    print(f"select() error: {exc}")
                    continue

                for conn in exceptional:
                    if conn is self.server_socket:
                        continue

                    self.close_connection(
                        conn.fileno(), "socket exception"
                    )

                for conn in readable:
                    if conn is self.server_socket:
                        self.accept_connections()
                        continue

                    fd = conn.fileno()
                    state = self.connections.get(fd)

                    if state is not None:
                        self.read_from_connection(fd, state)

                for conn in writable:
                    fd = conn.fileno()
                    state = self.connections.get(fd)

                    if state is not None:
                        self.write_to_connection(fd, state)

                # Start work for idle connections. This also lets a connection
                # claim another file after another connection finishes one.
                for fd, state in list(self.connections.items()):
                    if (
                        state["handshake_complete"]
                        and not state["output_buffer"]
                        and not state["awaiting_checkpoint"]
                        and not state["finish_after_write"]
                    ):
                        self.prepare_next_output(fd, state)

                self.save_metrics()

        except KeyboardInterrupt:
            print("\nStopping server...")

        finally:
            for fd in list(self.connections):
                self.close_connection(fd, "server shutting down")

            self.save_metrics()

            if self.server_socket is not None:
                self.read_sockets.discard(self.server_socket)
                self.write_sockets.discard(self.server_socket)
                self.server_socket.close()

            print("Server stopped.")


def main():
    parser = argparse.ArgumentParser(
        description="Windows-compatible non-blocking file server"
    )
    parser.add_argument("port", type=int)
    parser.add_argument("topics_root_dir")
    parser.add_argument(
        "failure_probability", type=float, nargs="?", default=0.0
    )
    parser.add_argument("--metrics", default="server_metrics.csv")
    args = parser.parse_args()

    server = NonBlockingServer(
        args.port,
        args.topics_root_dir,
        args.failure_probability,
        args.metrics,
    )
    server.run()


if __name__ == "__main__":
    main()
