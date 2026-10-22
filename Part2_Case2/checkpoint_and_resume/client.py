
import argparse
import json
import os
import socket
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
    decode_file_data,
)
from metrics import TransferMetrics

CHECKPOINT_INTERVAL = 1024 * 1024  # 1 MiB
SOCKET_TIMEOUT = 60
MAX_RETRIES = 100


def send_json(sock, message_type, data):
    send_frame(sock, message_type, encode_json(data))


def receive_json(sock, expected_type):
    message_type, payload = recv_frame(sock)

    if message_type == ERROR:
        error = decode_json(payload)
        raise RuntimeError(error.get("error", "Server error"))

    if message_type != expected_type:
        raise ProtocolError(
            f"Expected message type {expected_type}, got {message_type}"
        )

    return decode_json(payload)


def safe_filename(name):
    """Reject filenames that could escape the output directory."""
    return (
        isinstance(name, str)
        and bool(name)
        and name not in {".", ".."}
        and Path(name).name == name
        and "/" not in name
        and "\\" not in name
    )


def save_checkpoint(checkpoint_path, state):
    """Atomically persist the last server-confirmed checkpoint."""
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = checkpoint_path.with_suffix(
        checkpoint_path.suffix + ".tmp"
    )

    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(state, file)
        file.flush()
        os.fsync(file.fileno())

    os.replace(temporary_path, checkpoint_path)


def load_checkpoint(checkpoint_path, topic):
    """Load local recovery state, or return an empty initial state."""
    initial_state = {
        "topic": topic,
        "completed_files": [],
        "filename": None,
        "offset": 0,
    }

    if not checkpoint_path.exists():
        return initial_state

    try:
        with checkpoint_path.open("r", encoding="utf-8") as file:
            state = json.load(file)

        if state.get("topic") != topic:
            return initial_state

        completed = state.get("completed_files", [])
        filename = state.get("filename")
        offset = state.get("offset", 0)

        if (
            not isinstance(completed, list)
            or not all(safe_filename(name) for name in completed)
            or len(set(completed)) != len(completed)
            or (filename is not None and not safe_filename(filename))
            or not isinstance(offset, int)
            or isinstance(offset, bool)
            or offset < 0
            or (filename is None and offset != 0)
        ):
            return initial_state

        return {
            "topic": topic,
            "completed_files": completed,
            "filename": filename,
            "offset": offset,
        }

    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return initial_state


def connect(server_ip, server_port):
    sock = socket.create_connection(
        (server_ip, server_port),
        timeout=SOCKET_TIMEOUT,
    )
    sock.settimeout(SOCKET_TIMEOUT)
    return sock


def request_initial_transfer(sock, topic):
    send_json(sock, GET, {"topic": topic})

    response = receive_json(sock, RESUME_RESPONSE)

    if not response.get("accepted"):
        raise RuntimeError(
            response.get("reason", "Initial transfer request rejected")
        )


def request_resume(sock, state):
    send_json(
        sock,
        RESUME_REQUEST,
        {
            "topic": state["topic"],
            "completed_files": state["completed_files"],
            "filename": state["filename"],
            "offset": state["offset"],
        },
    )

    response = receive_json(sock, RESUME_RESPONSE)

    if response.get("restart"):
        # The server rejected the checkpoint. Restart cleanly.
        state["completed_files"] = []
        state["filename"] = None
        state["offset"] = 0

    elif not response.get("accepted"):
        raise RuntimeError("Server did not accept the resume request")

    else:
        # Use the server-validated offset, not an arbitrary local value.
        state["filename"] = response.get("filename")
        state["offset"] = response.get("offset", 0)

    return response


def truncate_to_confirmed_offset(part_path, offset):
    """
    Keep only confirmed bytes.

    Data written after the last checkpoint must be discarded before
    resuming, because the server may not have received its ACK.
    """
    part_path.parent.mkdir(parents=True, exist_ok=True)

    mode = "r+b" if part_path.exists() else "w+b"

    with part_path.open(mode) as file:
        file.truncate(offset)
        file.flush()
        os.fsync(file.fileno())


def receive_file(
    sock,
    topic,
    output_dir,
    state,
    metrics,
    expected_info=None,
):
    info = expected_info or receive_json(sock, FILE_INFO)

    filename = info.get("name")
    file_size = info.get("size")
    announced_offset = info.get("offset", 0)

    if not safe_filename(filename):
        raise ProtocolError("Server supplied an unsafe filename")

    if (
        not isinstance(file_size, int)
        or isinstance(file_size, bool)
        or file_size < 0
    ):
        raise ProtocolError("Invalid file size")

    if (
        not isinstance(announced_offset, int)
        or isinstance(announced_offset, bool)
        or not 0 <= announced_offset <= file_size
    ):
        raise ProtocolError("Invalid starting offset")

    # If this is the current checkpoint file, use the server-confirmed
    # offset. Otherwise the transfer should begin at byte zero.
    if state["filename"] == filename:
        confirmed_offset = state["offset"]
    else:
        confirmed_offset = 0

    if announced_offset != confirmed_offset:
        raise ProtocolError(
            f"Server offset mismatch for {filename}: "
            f"announced={announced_offset}, "
            f"expected={confirmed_offset}"
        )

    final_path = output_dir / filename
    part_path = output_dir / (filename + ".part")

    truncate_to_confirmed_offset(part_path, confirmed_offset)

    received_since_checkpoint = 0
    received_this_connection = 0
    current_offset = confirmed_offset

    state["filename"] = filename
    state["offset"] = confirmed_offset
    save_checkpoint(checkpoint_file_path(output_dir, topic), state)

    with part_path.open("r+b") as file:
        file.seek(confirmed_offset)

        while current_offset < file_size:
            message_type, payload = recv_frame(sock)

            if message_type == ERROR:
                error = decode_json(payload)
                raise RuntimeError(
                    error.get("error", "Server reported an error")
                )

            if message_type != FILE_DATA:
                raise ProtocolError(
                    f"Expected FILE_DATA, got {message_type}"
                )

            data_filename, data = decode_file_data(payload)

            if data_filename != filename:
                raise ProtocolError(
                    f"Expected data for {filename}, got {data_filename}"
                )

            if not data:
                raise ProtocolError("Received empty FILE_DATA chunk")

            if current_offset + len(data) > file_size:
                raise ProtocolError("Received more bytes than file size")

            # Write data to disk before acknowledging it.
            file.write(data)
            file.flush()

            current_offset += len(data)
            received_since_checkpoint += len(data)
            received_this_connection += len(data)

            metrics.update_file_progress(filename, current_offset)

            should_checkpoint = (
                received_since_checkpoint >= CHECKPOINT_INTERVAL
                or current_offset == file_size
            )

            if should_checkpoint:
                file.flush()
                os.fsync(file.fileno())

                ack = {
                    "topic": topic,
                    "filename": filename,
                    "offset": current_offset,
                }
                ack_payload = encode_json(ack)

                send_frame(sock, CHECKPOINT_ACK, ack_payload)

                # The client records its checkpoint after sending the ACK.
                # The server validates it before advancing its own offset.
                metrics.add_checkpoint_overhead(
                    HEADER_SIZE + len(ack_payload)
                )

                state["filename"] = filename
                state["offset"] = current_offset
                save_checkpoint(
                    checkpoint_file_path(output_dir, topic),
                    state,
                )

                received_since_checkpoint = 0

    if current_offset != file_size:
        raise ProtocolError("File size mismatch after receiving file")

    os.replace(part_path, final_path)

    # Mark the file complete in the persistent recovery state.
    if filename not in state["completed_files"]:
        state["completed_files"].append(filename)

    state["filename"] = None
    state["offset"] = 0

    save_checkpoint(checkpoint_file_path(output_dir, topic), state)

    # Count useful bytes only when the entire file is complete.
    metrics.add_useful_bytes(file_size)

    return received_this_connection


def checkpoint_file_path(output_dir, topic):
    return output_dir / f".{topic}.checkpoint.json"


def run_client(server_ip, server_port, topic, output_dir, metrics_csv):
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    checkpoint_path = checkpoint_file_path(output_path, topic)
    state = load_checkpoint(checkpoint_path, topic)

    metrics = TransferMetrics(topic, implementation="blocking")
    metrics.start()

    attempt = 0
    is_initial_request = True

    while attempt < MAX_RETRIES:
        attempt += 1
        sock = None

        try:
            sock = connect(server_ip, server_port)

            if is_initial_request:
                request_initial_transfer(sock, topic)
                is_initial_request = False
            else:
                request_resume(sock, state)
                metrics.record_reconnect()

                save_checkpoint(checkpoint_path, state)

            while True:
                message_type, payload = recv_frame(sock)

                if message_type == ERROR:
                    error = decode_json(payload)
                    raise RuntimeError(
                        error.get("error", "Server error")
                    )

                if message_type == TRANSFER_COMPLETE:
                    result = decode_json(payload)

                    if result.get("topic") != topic:
                        raise ProtocolError(
                            "TRANSFER_COMPLETE topic mismatch"
                        )

                    metrics.finish()

                    try:
                        checkpoint_path.unlink(missing_ok=True)
                    except OSError:
                        pass

                    metrics.append_csv(metrics_csv)

                    print("\nTransfer completed successfully.")
                    print(f"Topic: {topic}")
                    print(f"Output directory: {output_path}")
                    print("\nMetrics:")

                    for key, value in metrics.summary().items():
                        print(f"{key}: {value}")

                    return

                if message_type == FILE_INFO:
                    info = decode_json(payload)

                    # receive_file normally reads FILE_INFO itself.
                    # Pass the already-read metadata to avoid consuming
                    # the next frame accidentally.
                    receive_file(
                        sock,
                        topic,
                        output_path,
                        state,
                        metrics,
                        expected_info=info,
                    )
                    continue

                raise ProtocolError(
                    f"Unexpected message type {message_type}"
                )

        except (
            ConnectionError,
            OSError,
            TimeoutError,
            RuntimeError,
            ProtocolError,
            ValueError,
        ) as exc:
            print(f"Attempt {attempt} failed: {exc}")

            if attempt >= MAX_RETRIES:
                break

            metrics.record_failure()

            # Retain the last persisted checkpoint. Any unacknowledged
            # data is truncated when the file is resumed.
            time.sleep(min(0.25 * attempt, 3.0))

        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

    metrics.finish()
    metrics.append_csv(metrics_csv)

    raise RuntimeError(
        f"Transfer failed after {MAX_RETRIES} connection attempts"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Checkpoint-and-resume file distribution client"
    )
    parser.add_argument("server_ip")
    parser.add_argument("server_port", type=int)
    parser.add_argument("topic")
    parser.add_argument("output_dir")
    parser.add_argument(
        "--metrics",
        default="client_metrics.csv",
        help="CSV path for client transfer metrics",
    )

    args = parser.parse_args()

    run_client(
        args.server_ip,
        args.server_port,
        args.topic,
        args.output_dir,
        args.metrics,
    )


if __name__ == "__main__":
    main()
