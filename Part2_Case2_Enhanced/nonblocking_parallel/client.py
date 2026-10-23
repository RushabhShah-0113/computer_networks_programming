
"""Non-blocking parallel file-distribution client for Windows.

One event loop manages multiple TCP connections using select.select().
Supports checkpoints, resume requests, partial reads/writes, and metrics.
"""

import argparse
import csv
import json
import os
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
    MAX_PAYLOAD_SIZE,
    HEADER_SIZE,
    ProtocolError,
    encode_json_frame,
    decode_json,
    decode_file_data,
    extract_frames,
)


READ_SIZE = 64 * 1024
CHECKPOINT_INTERVAL = 1024 * 1024
SELECT_TIMEOUT = 0.1
MAX_RETRIES = 100


class NonBlockingClient:
    def __init__(
        self,
        server_ip,
        server_port,
        topic,
        output_dir,
        parallel_connections=3,
        metrics_csv="client_metrics.csv",
    ):
        if parallel_connections < 1:
            raise ValueError("parallel_connections must be at least 1")

        self.server_ip = server_ip
        self.server_port = server_port
        self.topic = topic
        self.output_dir = Path(output_dir).resolve() / topic
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.parallel_connections = parallel_connections
        self.metrics_csv = Path(metrics_csv)

        self.connections = {}
        self.read_sockets = set()
        self.write_sockets = set()

        self.completed_files = set()
        self.total_useful_bytes = 0
        self.total_transmitted_bytes = 0
        self.total_failures = 0
        self.total_reconnects = 0

        self.start_time = None
        self.end_time = None
        self.running = True

    # ---------------------------------------------------------
    # Persistent checkpoint state
    # ---------------------------------------------------------

    def checkpoint_path(self, worker_id):
        return self.output_dir / (
            f".worker_{worker_id}.checkpoint.json"
        )

    def load_checkpoint(self, worker_id):
        path = self.checkpoint_path(worker_id)

        if not path.exists():
            return {"filename": None, "offset": 0}

        try:
            with path.open("r", encoding="utf-8") as file:
                data = json.load(file)

            filename = data.get("filename")
            offset = data.get("offset", 0)

            if (
                data.get("topic") != self.topic
                or not isinstance(offset, int)
                or offset < 0
                or (
                    filename is not None
                    and (
                        not isinstance(filename, str)
                        or filename in ("", ".", "..")
                        or "/" in filename
                        or "\\" in filename
                    )
                )
            ):
                raise ValueError("Invalid checkpoint fields")

            if filename is None:
                offset = 0

            return {"filename": filename, "offset": offset}

        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return {"filename": None, "offset": 0}

    def save_checkpoint(self, worker):
        path = self.checkpoint_path(worker["id"])
        temporary = path.with_suffix(".tmp")

        data = {
            "topic": self.topic,
            "filename": worker["filename"],
            "offset": worker["confirmed_offset"],
        }

        with temporary.open("w", encoding="utf-8") as file:
            json.dump(data, file)
            file.flush()
            os.fsync(file.fileno())

        os.replace(temporary, path)

    def clear_checkpoint(self, worker):
        worker["filename"] = None
        worker["confirmed_offset"] = 0
        worker["current_offset"] = 0
        worker["bytes_since_checkpoint"] = 0
        self.save_checkpoint(worker)

    # ---------------------------------------------------------
    # Connection setup and reconnect
    # ---------------------------------------------------------

    def create_worker(self, worker_id):
        checkpoint = self.load_checkpoint(worker_id)

        return {
            "id": worker_id,
            "socket": None,
            "input_buffer": bytearray(),
            "output_buffer": bytearray(),
            "output_action": None,
            "filename": checkpoint["filename"],
            "confirmed_offset": checkpoint["offset"],
            "current_offset": checkpoint["offset"],
            "file_size": 0,
            "file_handle": None,
            "part_path": None,
            "bytes_since_checkpoint": 0,
            "handshake_complete": False,
            "awaiting_resume_response": False,
            "awaiting_checkpoint": False,
            "pending_checkpoint_offset": None,
            "first_connection": True,
            "retry_count": 0,
            "retry_at": 0.0,
            "finished": False,
            "connection_started": None,
            "useful_bytes": 0,
            "transmitted_bytes": 0,
            "checkpoint_count": 0,
            "checkpoint_overhead_bytes": 0,
            "checkpoint_processing_seconds": 0.0,
            "reconnect_count": 0,
            "failure_count": 0,
            "completed_files": set(),
        }

    def connect_worker(self, worker):
        if worker["finished"] or worker["socket"] is not None:
            return

        if worker["retry_count"] >= MAX_RETRIES:
            raise ConnectionError(
                f"Worker {worker['id']} exceeded retry limit"
            )

        try:
            # Establish the connection, then use non-blocking I/O.
            sock = socket.create_connection(
                (self.server_ip, self.server_port),
                timeout=5,
            )
            sock.setblocking(False)

        except OSError as exc:
            worker["retry_count"] += 1
            worker["retry_at"] = time.perf_counter() + min(
                0.1 * worker["retry_count"], 2.0
            )
            print(f"Worker {worker['id']}: connect failed: {exc}")
            return

        worker["socket"] = sock
        worker["input_buffer"] = bytearray()
        worker["output_buffer"] = bytearray()
        worker["handshake_complete"] = False
        worker["awaiting_resume_response"] = True
        worker["awaiting_checkpoint"] = False
        worker["pending_checkpoint_offset"] = None
        worker["connection_started"] = time.perf_counter()

        self.read_sockets.add(sock)

        if worker["first_connection"] and worker["filename"] is None:
            message_type = GET
            payload = {"topic": self.topic}
        else:
            message_type = RESUME_REQUEST
            payload = {
                "topic": self.topic,
                "filename": worker["filename"],
                "offset": worker["confirmed_offset"],
            }

        self.queue_json(worker, message_type, payload)
        worker["first_connection"] = False

        if worker["retry_count"] > 0:
            worker["reconnect_count"] += 1
            self.total_reconnects += 1

        print(f"Worker {worker['id']}: connected")

    def close_worker(self, worker, reason, retry=True):
        sock = worker["socket"]
        if sock is None:
            return

        self.read_sockets.discard(sock)
        self.write_sockets.discard(sock)

        try:
            sock.close()
        except OSError:
            pass

        worker["socket"] = None
        worker["input_buffer"] = bytearray()
        worker["output_buffer"] = bytearray()
        worker["output_action"] = None
        worker["handshake_complete"] = False
        worker["awaiting_resume_response"] = False
        worker["awaiting_checkpoint"] = False
        worker["pending_checkpoint_offset"] = None

        if worker["file_handle"] is not None:
            try:
                worker["file_handle"].close()
            except OSError:
                pass
            worker["file_handle"] = None

        if retry and not worker["finished"]:
            worker["retry_count"] += 1
            worker["failure_count"] += 1
            self.total_failures += 1
            worker["retry_at"] = time.perf_counter() + min(
                0.1 * worker["retry_count"], 2.0
            )

            print(
                f"Worker {worker['id']}: {reason}; "
                f"retry {worker['retry_count']}/{MAX_RETRIES}"
            )
        else:
            print(f"Worker {worker['id']}: {reason}")

    # ---------------------------------------------------------
    # Buffered output
    # ---------------------------------------------------------

    def queue_bytes(self, worker, data, action=None):
        if worker["output_buffer"]:
            raise RuntimeError("Output queue is unexpectedly non-empty")

        worker["output_buffer"].extend(data)
        worker["output_action"] = action

        sock = worker["socket"]
        if sock is not None:
            self.write_sockets.add(sock)

    def queue_json(self, worker, message_type, payload, action=None):
        self.queue_bytes(
            worker,
            encode_json_frame(message_type, payload),
            action=action,
        )

    def write_to_worker(self, worker):
        sock = worker["socket"]
        if sock is None:
            return

        while worker["output_buffer"]:
            try:
                sent = sock.send(worker["output_buffer"])
            except BlockingIOError:
                return
            except OSError as exc:
                self.close_worker(worker, f"write error: {exc}")
                return

            if sent <= 0:
                self.close_worker(worker, "socket send returned zero")
                return

            del worker["output_buffer"][:sent]

        self.write_sockets.discard(sock)

        action = worker["output_action"]
        worker["output_action"] = None

        if action is not None:
            self.after_output_sent(worker, action)

    def after_output_sent(self, worker, action):
        if action["type"] == "checkpoint_ack":
            worker["pending_checkpoint_offset"] = action["offset"]

            # Persist only after the ACK has been fully sent.
            worker["confirmed_offset"] = action["offset"]
            self.save_checkpoint(worker)

    # ---------------------------------------------------------
    # Input parsing and protocol dispatch
    # ---------------------------------------------------------

    def read_from_worker(self, worker):
        sock = worker["socket"]
        if sock is None:
            return

        try:
            while worker["socket"] is sock:
                data = sock.recv(READ_SIZE)

                if not data:
                    self.close_worker(worker, "server disconnected")
                    return

                worker["input_buffer"].extend(data)

                frames, remaining = extract_frames(worker["input_buffer"])
                worker["input_buffer"] = remaining

                if len(remaining) > MAX_PAYLOAD_SIZE + HEADER_SIZE:
                    raise ProtocolError("Input buffer exceeds frame limit")

                for message_type, payload in frames:
                    if worker["socket"] is not sock:
                        return
                    self.handle_frame(worker, message_type, payload)

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
            self.close_worker(worker, f"read/protocol error: {exc}")

    def handle_frame(self, worker, message_type, payload):
        if message_type == RESUME_RESPONSE:
            self.handle_resume_response(worker, decode_json(payload))

        elif message_type == FILE_INFO:
            self.handle_file_info(worker, decode_json(payload))

        elif message_type == FILE_DATA:
            filename, chunk = self.decode_file_chunk(payload)
            self.handle_file_data(worker, filename, chunk)

        elif message_type == TRANSFER_COMPLETE:
            response = decode_json(payload)

            if response.get("topic") != self.topic:
                raise ProtocolError("TRANSFER_COMPLETE topic mismatch")

            if (
                worker["file_handle"] is not None
                and worker["current_offset"] == worker["file_size"]
            ):
                self.finish_received_file(worker)

            self.finish_worker(worker)

            # Mark all idle workers without active files as finished
            for other_worker in self.connections.values():
                if other_worker.get("file_handle") is None:
                    other_worker["finished"] = True

        elif message_type == ERROR:
            response = decode_json(payload)
            raise ProtocolError(response.get("error", "Server error"))

        else:
            raise ProtocolError(
                f"Unexpected server message type: {message_type}"
            )

    def handle_resume_response(self, worker, response):
        if not worker["awaiting_resume_response"]:
            raise ProtocolError("Unexpected RESUME_RESPONSE")

        worker["awaiting_resume_response"] = False

        if not response.get("accepted", False):
            # If the checkpoint was rejected, clear it and reconnect.
            # The server closes this connection after its response.
            if response.get("restart", False):
                self.clear_checkpoint(worker)
                self.close_worker(worker, "resume rejected; restarting")
                return

            raise ProtocolError("Server rejected request")

        worker["handshake_complete"] = True

    # ---------------------------------------------------------
    # File reception and checkpoint ACKs
    # ---------------------------------------------------------

    def handle_file_info(self, worker, info):
        filename = info.get("name")
        size = info.get("size")
        offset = info.get("offset")

        if (
            not isinstance(filename, str)
            or filename in ("", ".", "..")
            or "/" in filename
            or "\\" in filename
            or not isinstance(size, int)
            or size < 0
            or not isinstance(offset, int)
            or offset < 0
            or offset > size
        ):
            raise ProtocolError("Invalid FILE_INFO")

        if worker["file_handle"] is not None:
            if worker["current_offset"] == worker["file_size"]:
                self.finish_received_file(worker)
            else:
                raise ProtocolError("Received FILE_INFO while a file is active")

        destination = self.output_dir / filename
        part_path = self.output_dir / f"{filename}.part"

        if destination.exists() and destination.stat().st_size == size:
            worker["completed_files"].add(filename)
            return

        handle = open(part_path, "r+b" if part_path.exists() else "w+b")

        try:
            handle.truncate(offset)
            handle.seek(offset)
        except OSError:
            handle.close()
            raise

        worker["filename"] = filename
        worker["file_size"] = size
        worker["current_offset"] = offset
        worker["confirmed_offset"] = offset
        worker["bytes_since_checkpoint"] = 0
        worker["file_handle"] = handle
        worker["part_path"] = part_path
        worker["awaiting_checkpoint"] = False
        worker["pending_checkpoint_offset"] = None

        self.save_checkpoint(worker)

        # Empty files still need a checkpoint ACK at offset zero.
        if size == offset:
            self.queue_checkpoint_ack(worker, offset)

    def decode_file_chunk(self, payload):
        return decode_file_data(payload)

    def handle_file_data(self, worker, filename, chunk):
        if worker["file_handle"] is None:
            raise ProtocolError("Received FILE_DATA without FILE_INFO")

        if filename != worker["filename"]:
            raise ProtocolError("FILE_DATA filename mismatch")

        if worker["awaiting_checkpoint"]:
            raise ProtocolError("Received data while awaiting checkpoint ACK")

        if worker["current_offset"] + len(chunk) > worker["file_size"]:
            raise ProtocolError("Received more data than declared file size")

        handle = worker["file_handle"]
        handle.write(chunk)

        worker["current_offset"] += len(chunk)
        worker["bytes_since_checkpoint"] += len(chunk)
        worker["transmitted_bytes"] += len(chunk)
        self.total_transmitted_bytes += len(chunk)

        is_eof = worker["current_offset"] == worker["file_size"]

        if (
            worker["bytes_since_checkpoint"] >= CHECKPOINT_INTERVAL
            or is_eof
        ):
            handle.flush()
            os.fsync(handle.fileno())
            self.queue_checkpoint_ack(
                worker, worker["current_offset"]
            )

    def queue_checkpoint_ack(self, worker, offset):
        payload = {
            "topic": self.topic,
            "filename": worker["filename"],
            "offset": offset,
        }

        worker["checkpoint_overhead_bytes"] += len(
            json.dumps(payload).encode("utf-8")
        )

        self.queue_json(
            worker,
            CHECKPOINT_ACK,
            payload,
            action={"type": "checkpoint_ack", "offset": offset},
        )

        worker["bytes_since_checkpoint"] = 0

    def finish_received_file(self, worker):
        handle = worker["file_handle"]
        filename = worker["filename"]

        if handle is None:
            return

        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        worker["file_handle"] = None

        destination = self.output_dir / filename
        os.replace(worker["part_path"], destination)

        if filename not in worker["completed_files"]:
            worker["completed_files"].add(filename)
            worker["useful_bytes"] += worker["file_size"]
            self.total_useful_bytes += worker["file_size"]

        worker["filename"] = None
        worker["confirmed_offset"] = 0
        worker["current_offset"] = 0
        worker["file_size"] = 0
        worker["part_path"] = None
        worker["bytes_since_checkpoint"] = 0
        worker["awaiting_checkpoint"] = False
        worker["pending_checkpoint_offset"] = None
        self.save_checkpoint(worker)

        print(f"Worker {worker['id']}: completed {filename}")

    # ---------------------------------------------------------
    # Worker completion and metrics
    # ---------------------------------------------------------

    def finish_worker(self, worker):
        if worker["finished"]:
            return

        worker["finished"] = True

        if worker["file_handle"] is not None:
            worker["file_handle"].close()
            worker["file_handle"] = None

        self.close_worker(
            worker,
            "topic transfer complete",
            retry=False,
        )

    def save_metrics(self):
        elapsed = max(
            (self.end_time or time.perf_counter()) - self.start_time,
            1e-9,
        )

        row = {
            "implementation": "nonblocking_select",
            "topic": self.topic,
            "parallel_connections": self.parallel_connections,
            "elapsed_seconds": round(elapsed, 6),
            "total_bytes_transmitted": self.total_transmitted_bytes,
            "useful_bytes": self.total_useful_bytes,
            "redundant_bytes": max(
                self.total_transmitted_bytes - self.total_useful_bytes, 0
            ),
            "throughput_mbps": round(
                self.total_useful_bytes * 8 / elapsed / 1_000_000, 6
            ),
            "raw_payload_throughput_mbps": round(
                self.total_transmitted_bytes * 8 / elapsed / 1_000_000, 6
            ),
            "failure_count": self.total_failures,
            "reconnect_count": self.total_reconnects,
        }

        self.metrics_csv.parent.mkdir(parents=True, exist_ok=True)
        exists = (
            self.metrics_csv.exists()
            and self.metrics_csv.stat().st_size > 0
        )

        with self.metrics_csv.open(
            "a", newline="", encoding="utf-8"
        ) as file:
            writer = csv.DictWriter(file, fieldnames=list(row.keys()))

            if not exists:
                writer.writeheader()

            writer.writerow(row)

        print("\nTransfer metrics")
        for key, value in row.items():
            print(f"{key}: {value}")

    # ---------------------------------------------------------
    # Main event loop
    # ---------------------------------------------------------

    def run(self):
        self.start_time = time.perf_counter()

        self.connections = {
            worker_id: self.create_worker(worker_id)
            for worker_id in range(self.parallel_connections)
        }

        try:
            while self.running:
                now = time.perf_counter()

                for worker in self.connections.values():
                    if (
                        not worker["finished"]
                        and worker["socket"] is None
                        and now >= worker["retry_at"]
                    ):
                        self.connect_worker(worker)

                active = [
                    worker
                    for worker in self.connections.values()
                    if not worker["finished"]
                ]

                if not active:
                    break

                read_list = list(self.read_sockets)
                write_list = list(self.write_sockets)
                exceptional_list = read_list + write_list

                if not read_list and not write_list:
                    time.sleep(SELECT_TIMEOUT)
                    continue

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

                for sock in exceptional:
                    worker = next(
                        (
                            w for w in self.connections.values()
                            if w["socket"] is sock
                        ),
                        None,
                    )
                    if worker is not None:
                        self.close_worker(worker, "socket exception")

                for sock in readable:
                    worker = next(
                        (
                            w for w in self.connections.values()
                            if w["socket"] is sock
                        ),
                        None,
                    )
                    if worker is not None:
                        self.read_from_worker(worker)

                for sock in writable:
                    worker = next(
                        (
                            w for w in self.connections.values()
                            if w["socket"] is sock
                        ),
                        None,
                    )
                    if worker is not None:
                        self.write_to_worker(worker)

                # Finalize a file only after its final checkpoint ACK has
                # actually been sent and the server has returned FILE_INFO
                # or TRANSFER_COMPLETE on a later event.
                for worker in self.connections.values():
                    if (
                        worker["file_handle"] is not None
                        and worker["current_offset"] == worker["file_size"]
                        and not worker["output_buffer"]
                    ):
                        self.finish_received_file(worker)

                if all(
                    worker["finished"]
                    for worker in self.connections.values()
                ):
                    break

                # Prevent an endless retry loop from hiding a failed run.
                if any(
                    worker["retry_count"] >= MAX_RETRIES
                    and not worker["finished"]
                    and worker["socket"] is None
                    for worker in self.connections.values()
                ):
                    raise ConnectionError("One or more workers exhausted retries")

        except KeyboardInterrupt:
            print("\nClient interrupted.")
            raise
        finally:
            self.end_time = time.perf_counter()

            for worker in self.connections.values():
                if worker["socket"] is not None:
                    self.close_worker(
                        worker, "client shutting down", retry=False
                    )

            self.save_metrics()


def main():
    parser = argparse.ArgumentParser(
        description="Non-blocking parallel file-distribution client"
    )
    parser.add_argument("server_ip")
    parser.add_argument("server_port", type=int)
    parser.add_argument("topic")
    parser.add_argument("output_dir")
    parser.add_argument("parallel_connections", type=int, nargs="?", default=3)
    parser.add_argument("--metrics", default="client_metrics.csv")

    args = parser.parse_args()

    client = NonBlockingClient(
        args.server_ip,
        args.server_port,
        args.topic,
        args.output_dir,
        args.parallel_connections,
        args.metrics,
    )
    client.run()


if __name__ == "__main__":
    main()
