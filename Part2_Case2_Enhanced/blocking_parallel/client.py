
import argparse
import json
import os
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
    encode_json,
    decode_json,
    decode_file_data,
)
from metrics import TransferMetrics


CHECKPOINT_INTERVAL = 1024 * 1024
SOCKET_TIMEOUT = 60
MAX_RETRIES = 100


class BlockingParallelClient:
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
            raise ValueError("At least one connection is required")

        self.server_ip = server_ip
        self.server_port = server_port
        self.topic = topic
        self.output_dir = Path(output_dir).resolve() / topic
        self.parallel_connections = parallel_connections
        self.metrics_csv = metrics_csv

        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.metrics = TransferMetrics(
            topic,
            implementation="blocking_parallel",
        )

        self.lock = threading.Lock()
        self.completed_files = set()
        self.worker_errors = []
        self.successful_workers = 0

    # --------------------------------------------------
    # Persistent checkpoint state
    # --------------------------------------------------

    def checkpoint_path(self, worker_id):
        return self.output_dir / (
            f".{self.topic}.worker{worker_id}.checkpoint.json"
        )

    def load_state(self, worker_id):
        path = self.checkpoint_path(worker_id)

        if not path.exists():
            return {
                "topic": self.topic,
                "filename": None,
                "offset": 0,
            }

        try:
            with path.open("r", encoding="utf-8") as file:
                state = json.load(file)

            filename = state.get("filename")
            offset = state.get("offset", 0)

            if (
                state.get("topic") != self.topic
                or not isinstance(offset, int)
                or isinstance(offset, bool)
                or offset < 0
                or (
                    filename is not None
                    and (
                        not isinstance(filename, str)
                        or filename in {"", ".", ".."}
                        or "/" in filename
                        or "\\" in filename
                    )
                )
            ):
                raise ValueError("Invalid checkpoint state")

            return {
                "topic": self.topic,
                "filename": filename,
                "offset": offset,
            }

        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return {
                "topic": self.topic,
                "filename": None,
                "offset": 0,
            }

    def save_state(self, worker_id, state):
        path = self.checkpoint_path(worker_id)
        temporary_path = path.with_suffix(path.suffix + ".tmp")

        with temporary_path.open("w", encoding="utf-8") as file:
            json.dump(state, file)
            file.flush()
            os.fsync(file.fileno())

        os.replace(temporary_path, path)

    # --------------------------------------------------
    # Connections and requests
    # --------------------------------------------------

    def connect(self):
        sock = socket.create_connection(
            (self.server_ip, self.server_port),
            timeout=SOCKET_TIMEOUT,
        )
        sock.settimeout(SOCKET_TIMEOUT)
        return sock

    def receive_json(self, sock, expected_type):
        message_type, payload = recv_frame(sock)

        if message_type == ERROR:
            error = decode_json(payload)
            raise RuntimeError(
                error.get("error", "Server error")
                if isinstance(error, dict)
                else "Server error"
            )

        if message_type != expected_type:
            raise ProtocolError(
                f"Expected message {expected_type}, got {message_type}"
            )

        result = decode_json(payload)

        if not isinstance(result, dict):
            raise ProtocolError("Expected JSON object")

        return result

    def send_initial_request(self, sock):
        send_json(sock, GET, {"topic": self.topic})

        response = self.receive_json(sock, RESUME_RESPONSE)

        if not response.get("accepted"):
            raise RuntimeError(
                response.get("reason", "Topic request rejected")
            )

    def send_resume_request(self, sock, state):
        send_json(
            sock,
            RESUME_REQUEST,
            {
                "topic": self.topic,
                "filename": state["filename"],
                "offset": state["offset"],
            },
        )

        response = self.receive_json(sock, RESUME_RESPONSE)

        if response.get("restart"):
            # The server rejected the local checkpoint.
            # Clear the active assignment and request work again.
            state["filename"] = None
            state["offset"] = 0
            return False

        if not response.get("accepted"):
            raise RuntimeError("Server rejected resume request")

        return True

    # --------------------------------------------------
    # File reception
    # --------------------------------------------------

    @staticmethod
    def validate_filename(filename):
        return (
            isinstance(filename, str)
            and filename not in {"", ".", ".."}
            and Path(filename).name == filename
            and "/" not in filename
            and "\\" not in filename
        )

    def receive_file(self, sock, worker_id, state, info):
        filename = info.get("name")
        size = info.get("size")
        server_offset = info.get("offset", 0)

        if not self.validate_filename(filename):
            raise ProtocolError("Unsafe filename received")

        if (
            not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
        ):
            raise ProtocolError("Invalid file size")

        if (
            not isinstance(server_offset, int)
            or isinstance(server_offset, bool)
            or not 0 <= server_offset <= size
        ):
            raise ProtocolError("Invalid file offset")

        final_path = self.output_dir / filename
        part_path = self.output_dir / (filename + ".part")

        # Do not overwrite a completed file merely because a reconnect
        # occurs after the server has already marked it complete.
        if (
            final_path.exists()
            and final_path.stat().st_size == size
            and server_offset == size
        ):
            with self.lock:
                self.completed_files.add(filename)
            return

        part_path.parent.mkdir(parents=True, exist_ok=True)

        mode = "r+b" if part_path.exists() else "w+b"

        with part_path.open(mode) as file:
            # Discard any data beyond the server-announced confirmed
            # offset. Those bytes might not have been checkpointed.
            file.truncate(server_offset)
            file.seek(server_offset)

            current_offset = server_offset
            bytes_since_checkpoint = 0

            state["filename"] = filename
            state["offset"] = server_offset
            self.save_state(worker_id, state)

            while current_offset < size:
                message_type, payload = recv_frame(sock)

                if message_type == ERROR:
                    error = decode_json(payload)
                    raise RuntimeError(
                        error.get("error", "Server error")
                    )

                if message_type != FILE_DATA:
                    raise ProtocolError(
                        f"Expected FILE_DATA, got {message_type}"
                    )

                data_filename, data = decode_file_data(payload)

                if data_filename != filename:
                    raise ProtocolError("FILE_DATA filename mismatch")

                if not data:
                    raise ProtocolError("Empty file-data chunk")

                if current_offset + len(data) > size:
                    raise ProtocolError("File data exceeds expected size")

                file.write(data)
                file.flush()

                current_offset += len(data)
                bytes_since_checkpoint += len(data)

                self.metrics.update_file_progress(
                    filename,
                    current_offset,
                )

                checkpoint_due = (
                    bytes_since_checkpoint >= CHECKPOINT_INTERVAL
                    or current_offset == size
                )

                if checkpoint_due:
                    file.flush()
                    os.fsync(file.fileno())

                    ack = {
                        "topic": self.topic,
                        "filename": filename,
                        "offset": current_offset,
                    }
                    ack_payload = encode_json(ack)

                    send_frame(sock, CHECKPOINT_ACK, ack_payload)

                    self.metrics.record_checkpoint_overhead(
                        HEADER_SIZE + len(ack_payload)
                    )

                    state["filename"] = filename
                    state["offset"] = current_offset
                    self.save_state(worker_id, state)

                    bytes_since_checkpoint = 0

        if part_path.stat().st_size != size:
            raise ProtocolError("Downloaded file size mismatch")

        os.replace(part_path, final_path)

        with self.lock:
            first_completion = filename not in self.completed_files
            self.completed_files.add(filename)

        if first_completion:
            self.metrics.record_useful_bytes(size)

        # No active file remains for this worker.
        state["filename"] = None
        state["offset"] = 0
        self.save_state(worker_id, state)

        print(
            f"[Worker {worker_id}] Received {filename} "
            f"({size} bytes)"
        )

    # --------------------------------------------------
    # Worker lifecycle
    # --------------------------------------------------

    def worker(self, worker_id):
        state = self.load_state(worker_id)
        first_connection = True
        attempt = 0

        while attempt < MAX_RETRIES:
            attempt += 1
            sock = None

            try:
                sock = self.connect()

                if first_connection and state["filename"] is None:
                    self.send_initial_request(sock)
                else:
                    self.send_resume_request(sock, state)

                first_connection = False

                while True:
                    message_type, payload = recv_frame(sock)

                    if message_type == ERROR:
                        error = decode_json(payload)
                        raise RuntimeError(
                            error.get("error", "Server error")
                        )

                    if message_type == FILE_INFO:
                        info = decode_json(payload)

                        if not isinstance(info, dict):
                            raise ProtocolError(
                                "Invalid FILE_INFO payload"
                            )

                        self.receive_file(
                            sock,
                            worker_id,
                            state,
                            info,
                        )
                        continue

                    if message_type == TRANSFER_COMPLETE:
                        result = decode_json(payload)

                        if result.get("topic") != self.topic:
                            raise ProtocolError(
                                "Topic completion mismatch"
                            )

                        with self.lock:
                            self.successful_workers += 1

                        print(
                            f"[Worker {worker_id}] Topic complete"
                        )
                        return

                    raise ProtocolError(
                        f"Unexpected message type {message_type}"
                    )

            except (
                OSError,
                ConnectionError,
                RuntimeError,
                ProtocolError,
                ValueError,
            ) as exc:
                print(
                    f"[Worker {worker_id}] Attempt {attempt} failed: {exc}"
                )

                self.metrics.record_failure()

                # Keep the last saved checkpoint. The server may have
                # accepted the ACK or may still have the previous offset.
                time.sleep(min(0.25 * attempt, 3.0))

            finally:
                if sock is not None:
                    try:
                        sock.close()
                    except OSError:
                        pass

        with self.lock:
            self.worker_errors.append(
                f"Worker {worker_id} exceeded retry limit"
            )

    # --------------------------------------------------
    # Run transfer
    # --------------------------------------------------

    def run(self):
        self.metrics.start()

        threads = []

        for worker_id in range(1, self.parallel_connections + 1):
            thread = threading.Thread(
                target=self.worker,
                args=(worker_id,),
                name=f"ClientWorker-{worker_id}",
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        if self.worker_errors:
            self.metrics.finish()
            self.metrics.append_csv(self.metrics_csv)

            raise RuntimeError("; ".join(self.worker_errors))

        if self.successful_workers != self.parallel_connections:
            self.metrics.finish()
            self.metrics.append_csv(self.metrics_csv)

            raise RuntimeError(
                "Not all client connections completed successfully"
            )

        self.metrics.finish()
        self.metrics.append_csv(self.metrics_csv)

        # Remove worker checkpoint files after successful completion.
        for worker_id in range(1, self.parallel_connections + 1):
            try:
                self.checkpoint_path(worker_id).unlink(missing_ok=True)
            except OSError:
                pass

        print("\nParallel transfer completed.")
        print(f"Topic: {self.topic}")
        print(f"Output directory: {self.output_dir}")
        print(f"Connections: {self.parallel_connections}")
        print("\nClient metrics:")

        for name, value in self.metrics.summary().items():
            print(f"{name}: {value}")


def main():
    parser = argparse.ArgumentParser(
        description="Blocking parallel file-transfer client"
    )
    parser.add_argument("server_ip")
    parser.add_argument("server_port", type=int)
    parser.add_argument("topic")
    parser.add_argument("output_dir")
    parser.add_argument(
        "parallel_connections",
        nargs="?",
        type=int,
        default=3,
    )
    parser.add_argument(
        "--metrics",
        default="client_metrics.csv",
    )

    args = parser.parse_args()

    client = BlockingParallelClient(
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
