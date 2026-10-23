
import argparse
import os
import socket

from protocol import (
    GET,
    FILE_INFO,
    FILE_DATA,
    TRANSFER_COMPLETE,
    ERROR,
    send_frame,
    recv_frame,
    encode_json,
    decode_json,
    decode_file_data,
)


def safe_filename(name):
    return (
        isinstance(name, str)
        and name not in ("", ".", "..")
        and os.path.basename(name) == name
        and "/" not in name
        and "\\" not in name
    )


def receive_topic(server_ip, server_port, topic, output_dir, timeout=300):
    topic_dir = os.path.join(output_dir, topic)
    os.makedirs(topic_dir, exist_ok=True)

    files_received = 0
    total_bytes_received = 0

    with socket.create_connection(
        (server_ip, server_port), timeout=timeout
    ) as sock:
        sock.settimeout(timeout)

        send_frame(
            sock,
            GET,
            encode_json({"topic": topic}),
        )

        while True:
            message_type, payload = recv_frame(sock)

            if message_type == ERROR:
                error = decode_json(payload)
                raise RuntimeError(
                    error.get("message", "Server reported an error")
                )

            if message_type == FILE_INFO:
                info = decode_json(payload)
                filename = info.get("name")
                expected_size = info.get("size")

                if not safe_filename(filename):
                    raise ValueError("Invalid filename in FILE_INFO")

                if (
                    not isinstance(expected_size, int)
                    or isinstance(expected_size, bool)
                    or expected_size < 0
                ):
                    raise ValueError("Invalid file size in FILE_INFO")

                final_path = os.path.join(topic_dir, filename)
                partial_path = final_path + ".part"
                received_for_file = 0

                try:
                    with open(partial_path, "wb") as file:
                        while received_for_file < expected_size:
                            data_type, data_payload = recv_frame(sock)

                            if data_type == ERROR:
                                error = decode_json(data_payload)
                                raise RuntimeError(
                                    error.get("message", "Transfer error")
                                )

                            if data_type != FILE_DATA:
                                raise ValueError(
                                    "Expected FILE_DATA frame"
                                )

                            data_filename, chunk = decode_file_data(
                                data_payload
                            )

                            if data_filename != filename:
                                raise ValueError(
                                    "FILE_DATA filename mismatch"
                                )

                            if not chunk:
                                raise ValueError(
                                    "Empty FILE_DATA chunk before file end"
                                )

                            if (
                                received_for_file + len(chunk)
                                > expected_size
                            ):
                                raise ValueError(
                                    "Received more bytes than FILE_INFO declared"
                                )

                            file.write(chunk)
                            received_for_file += len(chunk)
                            total_bytes_received += len(chunk)

                    if received_for_file != expected_size:
                        raise ValueError(
                            f"Incomplete file: {filename}"
                        )

                    os.replace(partial_path, final_path)
                    files_received += 1

                    print(
                        f"[RECEIVED] {filename}: "
                        f"{received_for_file} bytes"
                    )

                except Exception:
                    try:
                        if os.path.exists(partial_path):
                            os.remove(partial_path)
                    except OSError:
                        pass
                    raise

            elif message_type == TRANSFER_COMPLETE:
                result = decode_json(payload)

                if result.get("topic") != topic:
                    raise ValueError("Completed topic does not match request")

                if result.get("files_sent") != files_received:
                    raise ValueError("File count verification failed")

                if result.get("total_bytes") != total_bytes_received:
                    raise ValueError("Total byte count verification failed")

                print(f"[SUCCESS] Topic '{topic}' downloaded.")
                print(f"Files received: {files_received}")
                print(f"Useful bytes: {total_bytes_received}")

                return {
                    "files_received": files_received,
                    "useful_bytes": total_bytes_received,
                }

            else:
                raise ValueError(
                    f"Unexpected frame type: {message_type}"
                )


def main():
    parser = argparse.ArgumentParser(
        description="Topic file transfer client"
    )
    parser.add_argument("server_ip")
    parser.add_argument("server_port", type=int)
    parser.add_argument("topic")
    parser.add_argument("output_dir")
    parser.add_argument("--timeout", type=float, default=300.0)
    args = parser.parse_args()

    if not 1 <= args.server_port <= 65535:
        parser.error("Port must be between 1 and 65535")

    if not safe_filename(args.topic):
        parser.error("Invalid topic name")

    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    result = receive_topic(
        args.server_ip,
        args.server_port,
        args.topic,
        os.path.abspath(args.output_dir),
        args.timeout,
    )

    print(f"Transfer result: {result}")


if __name__ == "__main__":
    main()
