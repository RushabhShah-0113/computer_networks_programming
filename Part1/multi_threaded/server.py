"""Multi-threaded TCP topic-based file distribution server."""

import argparse
import os
import socket
import threading

from protocol import CHUNK_SIZE, recv_message, send_message


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

        # Transfer regular files only; do not follow symbolic links.
        if os.path.islink(path) or not os.path.isfile(path):
            continue

        files.append({
            "name": name,
            "path": path,
            "size": os.path.getsize(path),
        })

    return files


def handle_client(conn, address, root_dir):
    """Handle one client independently in its own thread."""
    print(f"[CONNECTED] {address} | Thread: {threading.current_thread().name}")

    with conn:
        try:
            request = recv_message(conn)

            if not isinstance(request, dict) or request.get("type") != "GET":
                send_message(conn, {
                    "type": "ERROR",
                    "message": "Expected a GET request",
                })
                return

            topic = request.get("topic")

            try:
                files = get_topic_files(root_dir, topic)
            except (ValueError, FileNotFoundError, OSError) as exc:
                send_message(conn, {
                    "type": "ERROR",
                    "message": str(exc),
                })
                return

            manifest = [
                {"name": item["name"], "size": item["size"]}
                for item in files
            ]

            send_message(conn, {
                "type": "TOPIC",
                "topic": topic,
                "files": manifest,
            })

            total_sent = 0

            for item in files:
                remaining = item["size"]

                with open(item["path"], "rb") as source:
                    while remaining > 0:
                        chunk = source.read(min(CHUNK_SIZE, remaining))

                        if not chunk:
                            raise OSError(
                                f"File changed or shortened during transfer: "
                                f"{item['name']}"
                            )

                        conn.sendall(chunk)
                        remaining -= len(chunk)
                        total_sent += len(chunk)

            send_message(conn, {
                "type": "TRANSFER_COMPLETE",
                "files": len(files),
                "payload_bytes": total_sent,
            })

            print(
                f"[COMPLETE] {address}: {len(files)} files, "
                f"{total_sent} payload bytes"
            )

        except (ConnectionError, OSError, ValueError) as exc:
            print(f"[TRANSFER ERROR] {address}: {exc}")


def main():
    parser = argparse.ArgumentParser(
        description="Multi-threaded TCP file distribution server"
    )
    parser.add_argument("port", type=int)
    parser.add_argument("topics_root_dir")
    args = parser.parse_args()

    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")

    root_dir = os.path.realpath(args.topics_root_dir)

    if not os.path.isdir(root_dir):
        parser.error(f"Topics directory does not exist: {root_dir}")

    # Track worker threads so they can be joined during shutdown.
    threads = []

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", args.port))
        server.listen()

        print(f"[LISTENING] Multi-threaded server on port {args.port}")
        print(f"[TOPICS ROOT] {root_dir}")

        try:
            while True:
                conn, address = server.accept()

                worker = threading.Thread(
                    target=handle_client,
                    args=(conn, address, root_dir),
                    name=f"Client-{address[0]}-{address[1]}",
                    daemon=True,
                )
                worker.start()
                threads.append(worker)

                # Periodically discard finished threads.
                threads = [thread for thread in threads if thread.is_alive()]

        except KeyboardInterrupt:
            print("\n[SHUTDOWN] Stopping server...")
        finally:
            for thread in threads:
                thread.join(timeout=2)


if __name__ == "__main__":
    main()