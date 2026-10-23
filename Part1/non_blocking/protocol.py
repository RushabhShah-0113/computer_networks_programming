
import json
import socket
import struct

HEADER_FORMAT = "!HIH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 8 bytes
CHUNK_SIZE = 64 * 1024
MAX_PAYLOAD_SIZE = 1024 * 1024

# Application frame type identifiers
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
    """Receive exactly size bytes from a TCP stream."""
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
    """Send an 8-byte typed header followed by the payload."""
    if message_type not in MESSAGE_NAMES:
        raise ValueError("Unknown message type")

    if not isinstance(payload, bytes):
        raise TypeError("Payload must be bytes")

    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ValueError("Payload exceeds maximum frame size")

    header = struct.pack(
        HEADER_FORMAT,
        message_type,
        len(payload),
        0,  # Reserved flags; currently always zero
    )

    sock.sendall(header + payload)


def recv_frame(sock):
    """Receive one complete typed frame."""
    header = recv_exact(sock, HEADER_SIZE)
    message_type, payload_length, flags = struct.unpack(
        HEADER_FORMAT, header
    )

    if message_type not in MESSAGE_NAMES:
        raise ValueError(f"Unknown message type: {message_type}")

    if payload_length > MAX_PAYLOAD_SIZE:
        raise ValueError("Payload exceeds maximum frame size")

    if flags != 0:
        raise ValueError("Unsupported flags")

    payload = recv_exact(sock, payload_length)

    return message_type, payload


def encode_json(message):
    return json.dumps(message).encode("utf-8")


def decode_json(payload):
    try:
        message = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid JSON control payload") from exc

    if not isinstance(message, dict):
        raise ValueError("Expected a JSON object")

    return message


def encode_file_data(filename, data):
    """
    FILE_DATA payload:
        2-byte filename length | filename UTF-8 | file chunk bytes
    """
    name_bytes = filename.encode("utf-8")

    if len(name_bytes) > 65535:
        raise ValueError("Filename is too long")

    payload = struct.pack("!H", len(name_bytes)) + name_bytes + data

    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ValueError("FILE_DATA payload exceeds maximum frame size")

    return payload


def decode_file_data(payload):
    """Return filename and binary chunk from a FILE_DATA payload."""
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
