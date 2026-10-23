"""Non-blocking event-driven TCP topic-based file distribution server using select.select()."""

import argparse
import collections
import os
import select
import socket
import struct

from protocol import (
    GET,
    FILE_INFO,
    FILE_DATA,
    TRANSFER_COMPLETE,
    ERROR,
    HEADER_FORMAT,
    HEADER_SIZE,
    CHUNK_SIZE,
    encode_json,
    decode_json,
    encode_file_data,
)


def get_topic_files(root_dir, topic):
    """Return safe, regular files directly inside the requested topic."""
    if not isinstance(topic, str) or not topic or topic in {".", ".."}:
        raise ValueError("Invalid topic name")

    if os.path.basename(topic) != topic or "/" in topic or "\\" in topic:
        raise ValueError("Invalid topic name")

    root = os.path.realpath(root_dir)
    topic_dir = os.path.realpath(os.path.join(root, topic))

    if os.path.commonpath([root, topic_dir]) != root:
        raise ValueError("Invalid topic path")

    if not os.path.isdir(topic_dir):
        raise FileNotFoundError(f"Topic not found: {topic}")

    files = []
    for name in sorted(os.listdir(topic_dir)):
        path = os.path.join(topic_dir, name)
        if os.path.islink(path) or not os.path.isfile(path):
            continue

        files.append({
            "name": name,
            "path": path,
            "size": os.path.getsize(path),
        })

    return files


def pack_frame(message_type, payload=b""):
    """Pack an application frame into raw bytes."""
    header = struct.pack(HEADER_FORMAT, message_type, len(payload), 0)
    return header + payload


class ClientSession:
    """Tracks non-blocking streaming state for one connected client."""

    def __init__(self, sock, address, root_dir):
        self.sock = sock
        self.address = address
        self.root_dir = root_dir
        self.fileno = sock.fileno()

        self.in_buffer = bytearray()
        self.out_buffer = bytearray()

        self.state = "AWAITING_GET"  # AWAITING_GET -> STREAMING -> FLUSHING -> DONE
        self.topic = None
        self.files = []
        self.file_index = 0
        self.active_file = None
        self.file_bytes_sent = 0
        self.total_bytes_sent = 0
        self.files_sent = 0

    def queue_frame(self, message_type, payload=b""):
        self.out_buffer.extend(pack_frame(message_type, payload))

    def queue_error(self, message):
        self.queue_frame(ERROR, encode_json({"message": str(message)}))
        self.state = "FLUSHING"

    def handle_read(self):
        """Read incoming bytes from client socket without blocking."""
        try:
            chunk = self.sock.recv(65536)
        except (BlockingIOError, InterruptedError):
            return True
        except (ConnectionError, OSError):
            return False

        if not chunk:
            return False  # Client disconnected

        self.in_buffer.extend(chunk)

        if self.state == "AWAITING_GET":
            while len(self.in_buffer) >= HEADER_SIZE:
                message_type, payload_length, _ = struct.unpack(
                    HEADER_FORMAT, self.in_buffer[:HEADER_SIZE]
                )
                if len(self.in_buffer) < HEADER_SIZE + payload_length:
                    break  # Wait for full payload

                payload = bytes(self.in_buffer[HEADER_SIZE : HEADER_SIZE + payload_length])
                del self.in_buffer[: HEADER_SIZE + payload_length]

                if message_type != GET:
                    self.queue_error("Expected GET request")
                    return True

                request = decode_json(payload)
                self.topic = request.get("topic")

                try:
                    self.files = get_topic_files(self.root_dir, self.topic)
                except (ValueError, FileNotFoundError, OSError) as exc:
                    self.queue_error(str(exc))
                    return True

                print(
                    f"[EVENT-LOOP] [SELECT] fd={self.fileno} ({self.address}): "
                    f"GET topic '{self.topic}' matched {len(self.files)} files"
                )

                self.state = "STREAMING"
                self.file_index = 0
                self._prepare_next_file()
                break

        return True

    def _prepare_next_file(self):
        if self.active_file:
            self.active_file.close()
            self.active_file = None

        if self.file_index < len(self.files):
            file_info = self.files[self.file_index]
            self.active_file = open(file_info["path"], "rb")
            self.file_bytes_sent = 0

            # Queue FILE_INFO frame
            self.queue_frame(
                FILE_INFO,
                encode_json({
                    "name": file_info["name"],
                    "size": file_info["size"],
                }),
            )
        else:
            # All files queued! Queue TRANSFER_COMPLETE
            total_size = sum(f["size"] for f in self.files)
            self.queue_frame(
                TRANSFER_COMPLETE,
                encode_json({
                    "topic": self.topic,
                    "files_sent": self.files_sent,
                    "total_bytes": total_size,
                }),
            )
            self.state = "FLUSHING"

    def handle_write(self):
        """Send queued bytes and buffer more file chunks without blocking."""
        # Top up out_buffer from the active file if below high-water mark (128 KB)
        while self.state == "STREAMING" and len(self.out_buffer) < 131072:
            if self.active_file is None:
                self._prepare_next_file()
                if self.state != "STREAMING":
                    break

            file_info = self.files[self.file_index]
            chunk = self.active_file.read(CHUNK_SIZE)

            if chunk:
                frame = pack_frame(FILE_DATA, encode_file_data(file_info["name"], chunk))
                self.out_buffer.extend(frame)
                self.file_bytes_sent += len(chunk)
                self.total_bytes_sent += len(chunk)
            else:
                # Finished current file
                self.files_sent += 1
                print(
                    f"[EVENT-LOOP] [SELECT] fd={self.fileno} ({self.address}): "
                    f"Sent {file_info['name']} ({self.file_bytes_sent} bytes)"
                )
                self.file_index += 1
                self._prepare_next_file()

        # Flush out_buffer to socket
        if self.out_buffer:
            try:
                sent = self.sock.send(self.out_buffer)
                del self.out_buffer[:sent]
            except (BlockingIOError, InterruptedError):
                pass
            except (ConnectionError, OSError):
                return False

        # If flushing and everything is sent, we are done
        if self.state == "FLUSHING" and not self.out_buffer:
            print(
                f"[EVENT-LOOP] [SELECT] [COMPLETE] fd={self.fileno} ({self.address}): "
                f"Completed topic '{self.topic}' ({self.files_sent} files, {self.total_bytes_sent} bytes)"
            )
            self.state = "DONE"
            return False

        return True

    def close(self):
        if self.active_file:
            try:
                self.active_file.close()
            except Exception:
                pass
            self.active_file = None

        try:
            self.sock.close()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(
        description="Non-blocking TCP file distribution server using select.select()"
    )
    parser.add_argument("port", type=int)
    parser.add_argument("topics_root_dir")
    args = parser.parse_args()

    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")

    root_dir = os.path.realpath(args.topics_root_dir)
    if not os.path.isdir(root_dir):
        parser.error(f"Topics directory does not exist: {root_dir}")

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.setblocking(False)
    server_sock.bind(("0.0.0.0", args.port))
    server_sock.listen(128)

    print(f"===============================================================")
    print(f" [NON-BLOCKING SERVER (select)] Listening on 0.0.0.0:{args.port}")
    print(f" [TOPICS ROOT] {root_dir}")
    print(f"===============================================================")

    sessions = {}  # socket -> ClientSession

    try:
        while True:
            read_sockets = [server_sock]
            write_sockets = []

            for sock, session in list(sessions.items()):
                # We want to read if client is awaiting request or flushing
                read_sockets.append(sock)

                # We want to write if there are bytes queued or file chunks to send
                if session.out_buffer or session.state == "STREAMING":
                    write_sockets.append(sock)

            readable, writable, exceptional = select.select(
                read_sockets, write_sockets, read_sockets, 0.1
            )

            # Handle new incoming connections
            for s in readable:
                if s is server_sock:
                    try:
                        conn, addr = server_sock.accept()
                        conn.setblocking(False)
                        sessions[conn] = ClientSession(conn, addr, root_dir)
                        print(
                            f"[EVENT-LOOP] [SELECT] [NEW CLIENT] {addr} connected. "
                            f"Total active clients multiplexed: {len(sessions)}"
                        )
                    except (BlockingIOError, InterruptedError):
                        pass
                else:
                    session = sessions.get(s)
                    if session and not session.handle_read():
                        print(f"[EVENT-LOOP] [SELECT] Client {session.address} disconnected")
                        session.close()
                        sessions.pop(s, None)

            # Handle writable sockets
            for s in writable:
                session = sessions.get(s)
                if session and not session.handle_write():
                    session.close()
                    sessions.pop(s, None)

            # Handle exceptional sockets
            for s in exceptional:
                session = sessions.get(s)
                if session:
                    print(f"[EVENT-LOOP] [SELECT] Socket error for {session.address}")
                    session.close()
                    sessions.pop(s, None)

    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Non-blocking server stopping...")
    finally:
        for session in list(sessions.values()):
            session.close()
        server_sock.close()


if __name__ == "__main__":
    main()
