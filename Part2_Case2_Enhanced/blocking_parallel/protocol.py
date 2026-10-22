
import json
import socket
import struct


# --------------------------------------------------
# Frame configuration
# --------------------------------------------------

HEADER_FORMAT = "!HIH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

CHUNK_SIZE = 64 * 1024
MAX_PAYLOAD_SIZE = 1024 * 1024
MAX_FILENAME_SIZE = 4096


# --------------------------------------------------
# Message types
# --------------------------------------------------

GET = 1
FILE_INFO = 2
FILE_DATA = 3
TRANSFER_COMPLETE = 4
ERROR = 5
CHECKPOINT_ACK = 6
RESUME_REQUEST = 7
RESUME_RESPONSE = 8


# --------------------------------------------------
# Protocol exceptions
# --------------------------------------------------

class ProtocolError(Exception):
    """Raised when a message violates the protocol."""
    pass


# --------------------------------------------------
# Reliable blocking socket helpers
# --------------------------------------------------

def send_all(sock, data):
    """
    Send every byte using a blocking TCP socket.

    Raises ConnectionError if the connection closes or fails.
    """
    view = memoryview(data)

    while view:
        try:
            sent = sock.send(view)
        except OSError as exc:
            raise ConnectionError("Socket send failed") from exc

        if sent == 0:
            raise ConnectionError("Connection closed during send")

        view = view[sent:]


def recv_exact(sock, size):
    """
    Receive exactly size bytes.

    TCP may return fewer bytes than requested in a single recv().
    """
    if size < 0:
        raise ProtocolError("Receive size cannot be negative")

    chunks = []
    remaining = size

    while remaining > 0:
        try:
            chunk = sock.recv(remaining)
        except OSError as exc:
            raise ConnectionError("Socket receive failed") from exc

        if not chunk:
            raise ConnectionError(
                "Connection closed before message was complete"
            )

        chunks.append(chunk)
        remaining -= len(chunk)

    return b"".join(chunks)


# --------------------------------------------------
# Frame encoding and decoding
# --------------------------------------------------

def send_frame(sock, message_type, payload=b""):
    """
    Send one frame:

        2 bytes: message type
        4 bytes: payload length
        2 bytes: reserved flags
        N bytes: payload
    """
    if not isinstance(payload, bytes):
        raise TypeError("Frame payload must be bytes")

    if not isinstance(message_type, int) or not 0 <= message_type <= 65535:
        raise ProtocolError("Invalid message type")

    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ProtocolError("Payload exceeds maximum size")

    header = struct.pack(
        HEADER_FORMAT,
        message_type,
        len(payload),
        0,
    )

    send_all(sock, header)
    send_all(sock, payload)


def recv_frame(sock):
    """Receive and decode one complete frame."""
    header = recv_exact(sock, HEADER_SIZE)

    message_type, payload_length, flags = struct.unpack(
        HEADER_FORMAT,
        header,
    )

    if flags != 0:
        raise ProtocolError("Unsupported frame flags")

    if payload_length > MAX_PAYLOAD_SIZE:
        raise ProtocolError("Payload exceeds maximum size")

    payload = recv_exact(sock, payload_length)

    return message_type, payload


# --------------------------------------------------
# JSON metadata helpers
# --------------------------------------------------

def encode_json(data):
    """Convert a JSON-compatible object to UTF-8 bytes."""
    try:
        return json.dumps(
            data,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProtocolError("Could not encode JSON message") from exc


def decode_json(payload):
    """Decode a UTF-8 JSON payload."""
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("Invalid JSON payload") from exc


def send_json(sock, message_type, data):
    """Encode and send a JSON metadata message."""
    send_frame(sock, message_type, encode_json(data))


def recv_json(sock, expected_type):
    """Receive a JSON message and validate its message type."""
    message_type, payload = recv_frame(sock)

    if message_type == ERROR:
        error = decode_json(payload)
        raise ProtocolError(
            error.get("error", "Server returned an error")
            if isinstance(error, dict)
            else "Server returned an error"
        )

    if message_type != expected_type:
        raise ProtocolError(
            f"Expected message type {expected_type}, "
            f"received {message_type}"
        )

    return decode_json(payload)


# --------------------------------------------------
# FILE_DATA payload helpers
# --------------------------------------------------

def encode_file_data(filename, data):
    """
    Encode a file-data payload:

        2 bytes: filename length
        N bytes: UTF-8 filename
        Remaining bytes: binary file data
    """
    if not isinstance(filename, str) or not filename:
        raise ProtocolError("Filename must be a non-empty string")

    filename_bytes = filename.encode("utf-8")

    if len(filename_bytes) > MAX_FILENAME_SIZE:
        raise ProtocolError("Filename exceeds maximum length")

    if not isinstance(data, bytes):
        raise TypeError("File data must be bytes")

    payload = (
        struct.pack("!H", len(filename_bytes))
        + filename_bytes
        + data
    )

    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ProtocolError("FILE_DATA payload exceeds maximum size")

    return payload


def decode_file_data(payload):
    """Decode a FILE_DATA payload into (filename, binary_data)."""
    if len(payload) < 2:
        raise ProtocolError("FILE_DATA payload is too short")

    filename_length = struct.unpack("!H", payload[:2])[0]

    if (
        filename_length == 0
        or filename_length > MAX_FILENAME_SIZE
        or len(payload) < 2 + filename_length
    ):
        raise ProtocolError("Invalid filename length")

    try:
        filename = payload[
            2:2 + filename_length
        ].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProtocolError("Filename is not valid UTF-8") from exc

    # Reject path traversal: this field is a filename, not a path.
    if (
        filename in {".", ".."}
        or "/" in filename
        or "\\" in filename
    ):
        raise ProtocolError("Invalid filename")

    data = payload[2 + filename_length:]

    return filename, data
