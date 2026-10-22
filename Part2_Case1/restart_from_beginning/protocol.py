import json
import socket
import struct

HEADER_FORMAT = "!HIH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 8 bytes
CHUNK_SIZE = 64 * 1024
MAX_PAYLOAD_SIZE = 1024 * 1024

GET = 1
FILE_INFO = 2
FILE_DATA = 3
TRANSFER_COMPLETE = 4
ERROR = 5

MESSAGE_NAMES = {
    GET: "GET",
    FILE_INFO: "FILE_INFO",
    FILE_DATA: "FILE_DATA",
    TRANSFER_COMPLETE: "TRANSFER_COMPLETE",
    ERROR: "ERROR",
}


def recv_exact(sock, size):
    if size < 0:
        raise ValueError("size cannot be negative")

    chunks = []
    remaining = size

    while remaining:
        chunk = sock.recv(min(remaining, CHUNK_SIZE))
        if not chunk:
            raise ConnectionError(
                f"Connection closed with {remaining} bytes remaining"
            )
        chunks.append(chunk)
        remaining -= len(chunk)

    return b"".join(chunks)


def send_frame(sock, message_type, payload=b""):
    if message_type not in MESSAGE_NAMES:
        raise ValueError("Unknown message type")
    if not isinstance(payload, bytes):
        raise TypeError("Payload must be bytes")
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ValueError("Payload exceeds maximum frame size")

    header = struct.pack(HEADER_FORMAT, message_type, len(payload), 0)
    sock.sendall(header + payload)


def recv_frame(sock):
    header = recv_exact(sock, HEADER_SIZE)
    message_type, payload_length, flags = struct.unpack(
        HEADER_FORMAT, header
    )

    if message_type not in MESSAGE_NAMES:
        raise ValueError(f"Unknown message type: {message_type}")
    if payload_length > MAX_PAYLOAD_SIZE:
        raise ValueError("Payload exceeds maximum frame size")
    if flags != 0:
        raise ValueError("Unsupported header flags")

    return message_type, recv_exact(sock, payload_length)


def encode_json(message):
    return json.dumps(message).encode("utf-8")


def decode_json(payload):
    try:
        result = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid JSON payload") from exc

    if not isinstance(result, dict):
        raise ValueError("Expected a JSON object")

    return result


def encode_file_data(filename, data):
    name = filename.encode("utf-8")
    if len(name) > 65535:
        raise ValueError("Filename too long")

    payload = struct.pack("!H", len(name)) + name + data
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ValueError("FILE_DATA payload too large")

    return payload


def decode_file_data(payload):
    if len(payload) < 2:
        raise ValueError("Invalid FILE_DATA payload")

    (name_length,) = struct.unpack("!H", payload[:2])
    if len(payload) < 2 + name_length:
        raise ValueError("Truncated FILE_DATA filename")

    try:
        filename = payload[2:2 + name_length].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Invalid filename encoding") from exc

    return filename, payload[2 + name_length:]