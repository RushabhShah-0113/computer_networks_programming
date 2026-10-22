
"""Message framing helpers for the TCP file distribution protocol."""

import json
import struct


HEADER_SIZE = 4
CHUNK_SIZE = 64 * 1024


def recv_exact(sock, size):
    """Receive exactly size bytes, or raise ConnectionError."""
    chunks = []
    remaining = size

    while remaining > 0:
        chunk = sock.recv(min(remaining, CHUNK_SIZE))

        if not chunk:
            raise ConnectionError("Connection closed before all bytes arrived")

        chunks.append(chunk)
        remaining -= len(chunk)

    return b"".join(chunks)


def send_message(sock, message):
    """Send a JSON message with a 4-byte network-order length prefix."""
    payload = json.dumps(message).encode("utf-8")
    header = struct.pack("!I", len(payload))

    sock.sendall(header + payload)


def recv_message(sock):
    """Receive and decode one length-prefixed JSON message."""
    header = recv_exact(sock, HEADER_SIZE)
    (payload_size,) = struct.unpack("!I", header)

    if payload_size == 0 or payload_size > 1024 * 1024:
        raise ValueError(f"Invalid message size: {payload_size}")

    payload = recv_exact(sock, payload_size)
    return json.loads(payload.decode("utf-8"))
