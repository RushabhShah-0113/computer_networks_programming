
import json
import struct

# Frame format:
#   2 bytes: message type
#   4 bytes: payload length
#   2 bytes: reserved flags
#
# All fields use network byte order (big-endian).

HEADER_FORMAT = "!HIH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

CHUNK_SIZE = 64 * 1024
MAX_PAYLOAD_SIZE = 1024 * 1024
MAX_FILENAME_SIZE = 4096

# Message types
GET = 1
FILE_INFO = 2
FILE_DATA = 3
TRANSFER_COMPLETE = 4
ERROR = 5
CHECKPOINT_ACK = 6
RESUME_REQUEST = 7
RESUME_RESPONSE = 8


class ProtocolError(Exception):
    """Raised when a peer sends an invalid protocol frame."""


def recv_exact(sock, size):
    """Receive exactly size bytes, or raise ConnectionError if disconnected."""
    if size < 0:
        raise ProtocolError("Negative receive size")

    chunks = []
    remaining = size

    while remaining:
        chunk = sock.recv(remaining)

        if not chunk:
            raise ConnectionError("Connection closed before frame was complete")

        chunks.append(chunk)
        remaining -= len(chunk)

    return b"".join(chunks)


def send_frame(sock, message_type, payload=b""):
    """Send one typed, length-prefixed protocol frame."""
    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes")

    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ProtocolError("Payload exceeds maximum allowed size")

    if not 0 <= message_type <= 0xFFFF:
        raise ProtocolError("Invalid message type")

    header = struct.pack(
        HEADER_FORMAT,
        message_type,
        len(payload),
        0,  # Reserved flags
    )

    sock.sendall(header + payload)


def recv_frame(sock):
    """Receive and validate one complete frame."""
    header = recv_exact(sock, HEADER_SIZE)
    message_type, payload_length, _flags = struct.unpack(
        HEADER_FORMAT, header
    )

    if payload_length > MAX_PAYLOAD_SIZE:
        raise ProtocolError(
            f"Payload length {payload_length} exceeds maximum"
        )

    payload = recv_exact(sock, payload_length)
    return message_type, payload


def encode_json(data):
    """Encode a JSON-compatible object as UTF-8 bytes."""
    return json.dumps(
        data,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def decode_json(payload):
    """Decode a JSON payload, converting malformed input to ProtocolError."""
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("Invalid JSON payload") from exc


def encode_file_data(filename, data):
    """
    Encode a FILE_DATA payload:
      2 bytes: filename length
      N bytes: UTF-8 filename
      remaining bytes: raw file data
    """
    filename_bytes = filename.encode("utf-8")

    if not filename_bytes:
        raise ProtocolError("Filename cannot be empty")

    if len(filename_bytes) > MAX_FILENAME_SIZE:
        raise ProtocolError("Filename is too long")

    if not isinstance(data, bytes):
        raise TypeError("File data must be bytes")

    payload = struct.pack("!H", len(filename_bytes))
    payload += filename_bytes
    payload += data

    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ProtocolError("FILE_DATA payload exceeds maximum size")

    return payload


def decode_file_data(payload):
    """Decode FILE_DATA into (filename, raw file bytes)."""
    if len(payload) < 2:
        raise ProtocolError("FILE_DATA payload is too short")

    filename_length = struct.unpack("!H", payload[:2])[0]

    if (
        filename_length == 0
        or filename_length > MAX_FILENAME_SIZE
        or len(payload) < 2 + filename_length
    ):
        raise ProtocolError("Invalid filename in FILE_DATA payload")

    try:
        filename = payload[2:2 + filename_length].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProtocolError("Invalid UTF-8 filename") from exc

    data = payload[2 + filename_length:]
    return filename, data
