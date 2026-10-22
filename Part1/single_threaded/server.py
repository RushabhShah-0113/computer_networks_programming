
import argparse
import os
import socket

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
    """List regular files directly inside a valid topic directory."""
    if (
        not isinstance(topic, str)
        or not topic
        or topic in (".", "..")
        or os.path.basename(topic) != topic
        or "/" in topic
        or "\\" in topic
    ):
        raise ValueError("Invalid topic name")

    root_real = os.path.realpath(root_dir)
    topic_dir = os.path.realpath(os.path.join(root_real, topic))

    if os.path.commonpath([root_real, topic_dir]) != root_real:
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
            "size": os.path.getsize(path),
            "path": path,
        })

    return topic_dir, files


def handle_client(conn, address, root_dir):
    print(f"[CONNECTED] {address}")

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
            topic_dir, files = get_topic_files(root_dir, topic)
        except (ValueError, FileNotFoundError) as exc:
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
            print(f"[SENT] {name}: {sent_for_file} bytes")

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
            f"[COMPLETE] {address}: topic={topic}, "
            f"files={files_sent}, bytes={bytes_sent}"
        )

    except (ConnectionError, OSError, ValueError) as exc:
        print(f"[TRANSFER ERROR] {address}: {exc}")

        # Best-effort error frame; the peer may already have disconnected.
        try:
            send_frame(
                conn,
                ERROR,
                encode_json({"message": str(exc)}),
            )
        except OSError:
            pass

    finally:
        conn.close()
        print(f"[DISCONNECTED] {address}")


def main():
    parser = argparse.ArgumentParser(
        description="Single-threaded topic file server"
    )
    parser.add_argument("port", type=int)
    parser.add_argument("topics_root_dir")
    args = parser.parse_args()

    if not 1 <= args.port <= 65535:
        parser.error("Port must be between 1 and 65535")

    root_dir = os.path.abspath(args.topics_root_dir)

    if not os.path.isdir(root_dir):
        parser.error(f"Topics directory does not exist: {root_dir}")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", args.port))
    server.listen()

    print(f"[LISTENING] 0.0.0.0:{args.port}")
    print(f"[TOPICS DIR] {root_dir}")

    try:
        while True:
            conn, address = server.accept()
            handle_client(conn, address, root_dir)
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Server stopping")
    finally:
        server.close()


if __name__ == "__main__":
    main()
