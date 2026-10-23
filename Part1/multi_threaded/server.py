"""Multi-threaded TCP topic-based file distribution server."""

import argparse
import os
import socket
import threading

from protocol import (
    GET,
    FILE_INFO,
    FILE_DATA,
    TRANSFER_COMPLETE,
    ERROR,
    CHUNK_SIZE,
    send_frame,
    recv_frame,
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
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] [CONNECTED] Client {address}")

    with conn:
        try:
            message_type, payload = recv_frame(conn)

            if message_type != GET:
                send_frame(
                    conn,
                    ERROR,
                    encode_json({"message": "Expected GET request"}),
                )
                return

            request = decode_json(payload)
            topic = request.get("topic")

            try:
                files = get_topic_files(root_dir, topic)
            except (ValueError, FileNotFoundError, OSError) as exc:
                send_frame(
                    conn,
                    ERROR,
                    encode_json({"message": str(exc)}),
                )
                return

            total_bytes = sum(item["size"] for item in files)
            files_sent = 0
            bytes_sent = 0

            for item in files:
                name = item["name"]
                size = item["size"]
                path = item["path"]

                # Announce the file and its total size.
                send_frame(
                    conn,
                    FILE_INFO,
                    encode_json({
                        "name": name,
                        "size": size,
                    }),
                )

                sent_for_file = 0

                with open(path, "rb") as file:
                    while True:
                        chunk = file.read(CHUNK_SIZE)
                        if not chunk:
                            break

                        file_payload = encode_file_data(name, chunk)
                        send_frame(conn, FILE_DATA, file_payload)

                        sent_for_file += len(chunk)
                        bytes_sent += len(chunk)

                if sent_for_file != size:
                    raise OSError(
                        f"File changed during transfer: {name}"
                    )

                files_sent += 1
                print(
                    f"[{thread_name}] [SENT] {name} ({sent_for_file} bytes) -> {address}"
                )

            send_frame(
                conn,
                TRANSFER_COMPLETE,
                encode_json({
                    "topic": topic,
                    "files_sent": files_sent,
                    "total_bytes": total_bytes,
                }),
            )

            print(
                f"[{thread_name}] [COMPLETE] Finished topic '{topic}' for {address}: "
                f"{files_sent} files, {bytes_sent} bytes transferred."
            )

        except (ConnectionError, OSError, ValueError) as exc:
            print(f"[{thread_name}] [TRANSFER ERROR] {address}: {exc}")
            try:
                send_frame(
                    conn,
                    ERROR,
                    encode_json({"message": str(exc)}),
                )
            except OSError:
                pass

        finally:
            print(f"[{thread_name}] [DISCONNECTED] {address}")


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
    worker_counter = 0

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", args.port))
        server.listen(128)

        print(f"===============================================================")
        print(f" [MULTI-THREADED SERVER] Listening on 0.0.0.0:{args.port}")
        print(f" [TOPICS ROOT] {root_dir}")
        print(f"===============================================================")

        try:
            while True:
                conn, address = server.accept()
                worker_counter += 1

                worker = threading.Thread(
                    target=handle_client,
                    args=(conn, address, root_dir),
                    name=f"Worker-{worker_counter}",
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