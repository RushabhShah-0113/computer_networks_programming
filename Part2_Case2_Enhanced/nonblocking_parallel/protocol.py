
"""Framing and message definitions for the non-blocking implementation."""

import json
import struct

HEADER_FORMAT = "!HIH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

CHUNK_SIZE = 64 * 1024
MAX_PAYLOAD_SIZE = 1024 * 1024
MAX_FILENAME_SIZE = 4096

GET = 1
FILE_INFO = 2
FILE_DATA = 3
TRANSFER_COMPLETE = 4
ERROR = 5
CHECKPOINT_ACK = 6
RESUME_REQUEST = 7
RESUME_RESPONSE = 8


class ProtocolError(Exception):
    """Raised when a received frame is malformed."""


def encode_frame(message_type, payload=b""):
    """Build one complete frame without performing socket I/O."""
    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes")

    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ProtocolError("Payload exceeds maximum permitted size")

    header = struct.pack(
        HEADER_FORMAT,
        message_type,
        len(payload),
        0,
    )
    return header + payload


def encode_json_frame(message_type, data):
    """Encode a JSON message as a complete frame."""
    payload = json.dumps(data).encode("utf-8")
    return encode_frame(message_type, payload)


def decode_json(payload):
    """Decode a JSON payload."""
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("Invalid JSON payload") from exc


def extract_frames(buffer):
    """
    Extract all complete frames from a bytearray.

    Returns:
        frames: list of (message_type, payload)
        remaining: bytearray containing any incomplete frame.
    """
    frames = []
    offset = 0
    total = len(buffer)

    while total - offset >= HEADER_SIZE:
        message_type, payload_length, flags = struct.unpack_from(
            HEADER_FORMAT, buffer, offset
        )

        if flags != 0:
            raise ProtocolError("Unsupported frame flags")

        if payload_length > MAX_PAYLOAD_SIZE:
            raise ProtocolError("Payload exceeds maximum permitted size")

        frame_end = offset + HEADER_SIZE + payload_length

        if frame_end > total:
            break

        payload_start = offset + HEADER_SIZE
        payload = bytes(buffer[payload_start:frame_end])
        frames.append((message_type, payload))
        offset = frame_end

    return frames, bytearray(buffer[offset:])


def encode_file_data(filename, chunk):
    """Encode a filename and file-data chunk."""
    filename_bytes = filename.encode("utf-8")

    if (
        not filename_bytes
        or len(filename_bytes) > MAX_FILENAME_SIZE
        or "/" in filename
        or "\\" in filename
        or filename in (".", "..")
    ):
        raise ProtocolError("Invalid filename")

    payload = struct.pack("!H", len(filename_bytes))
    payload += filename_bytes
    payload += chunk

    return encode_frame(FILE_DATA, payload)


def decode_file_data(payload):
    """Decode a FILE_DATA payload into (filename, chunk)."""
    if len(payload) < 2:
        raise ProtocolError("FILE_DATA payload is too short")

    filename_length = struct.unpack("!H", payload[:2])[0]

    if (
        filename_length == 0
        or filename_length > MAX_FILENAME_SIZE
        or len(payload) < 2 + filename_length
    ):
        raise ProtocolError("Invalid FILE_DATA filename length")

    try:
        filename = payload[2:2 + filename_length].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProtocolError("Filename is not valid UTF-8") from exc

    if (
        "/" in filename
        or "\\" in filename
        or filename in (".", "..")
    ):
        raise ProtocolError("Invalid filename")

    chunk = payload[2 + filename_length:]
    return filename, chunk
