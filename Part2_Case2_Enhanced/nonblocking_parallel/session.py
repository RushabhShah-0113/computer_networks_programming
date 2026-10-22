
import os
import threading
from pathlib import Path


class SessionManager:
    """
    Manages topic transfer state and server-confirmed checkpoints.

    File status:
        0 = available
        1 = currently being transferred
        2 = completed
    """

    def __init__(self, topics_root_dir):
        self.root = Path(topics_root_dir).resolve()

        self.lock = threading.RLock()

        # topic -> ordered list of filenames
        self.topic_files = {}

        # (topic, filename) -> {
        #     "size": int,
        #     "confirmed_offset": int,
        #     "status": int
        # }
        self.file_state = {}

    def _safe_topic_dir(self, topic):
        """Return a valid topic directory or raise ValueError."""
        if (
            not isinstance(topic, str)
            or not topic
            or topic in {".", ".."}
            or "/" in topic
            or "\\" in topic
        ):
            raise ValueError("Invalid topic name")

        topic_dir = (self.root / topic).resolve()

        if topic_dir.parent != self.root or not topic_dir.is_dir():
            raise ValueError("Topic directory does not exist")

        return topic_dir

    def register_topic(self, topic):
        """
        Discover topic files and initialize state.

        Re-registering an existing topic does not reset its checkpoints.
        """
        with self.lock:
            topic_dir = self._safe_topic_dir(topic)

            filenames = sorted(
                path.name
                for path in topic_dir.iterdir()
                if path.is_file() and not path.is_symlink()
            )

            if topic not in self.topic_files:
                self.topic_files[topic] = filenames

                for filename in filenames:
                    path = topic_dir / filename
                    self.file_state[(topic, filename)] = {
                        "size": path.stat().st_size,
                        "confirmed_offset": 0,
                        "status": 0,
                    }
            else:
                # Do not silently continue with a changed file list.
                if self.topic_files[topic] != filenames:
                    raise ValueError(
                        "Topic file list changed during transfer"
                    )

                # Check that existing source files have not changed size.
                for filename in filenames:
                    current_size = (topic_dir / filename).stat().st_size
                    state = self.file_state[(topic, filename)]

                    if current_size != state["size"]:
                        raise ValueError(
                            f"Source file size changed: {filename}"
                        )

            return list(self.topic_files[topic])

    def get_topic_files(self, topic):
        """Return the topic's deterministic file order."""
        with self.lock:
            if topic not in self.topic_files:
                self.register_topic(topic)

            return list(self.topic_files[topic])

    def get_file_state(self, topic, filename):
        """Return a copy of a file's current state."""
        with self.lock:
            key = (topic, filename)

            if key not in self.file_state:
                raise ValueError("Unknown topic or filename")

            return dict(self.file_state[key])

    def claim_file(self, topic, filename):
        """
        Atomically claim an available file.

        Returns its confirmed starting offset, or None if unavailable.
        """
        with self.lock:
            key = (topic, filename)

            if key not in self.file_state:
                raise ValueError("Unknown topic or filename")

            state = self.file_state[key]

            if state["status"] != 0:
                return None

            state["status"] = 1
            return state["confirmed_offset"]

    def acknowledge_checkpoint(self, topic, filename, offset):
        """
        Advance the confirmed offset only after CHECKPOINT_ACK is received.
        """
        with self.lock:
            key = (topic, filename)

            if key not in self.file_state:
                raise ValueError("Unknown topic or filename")

            state = self.file_state[key]
            old_offset = state["confirmed_offset"]
            size = state["size"]

            if state["status"] != 1:
                raise ValueError("File is not currently being transferred")

            if not isinstance(offset, int) or isinstance(offset, bool):
                raise ValueError("Checkpoint offset must be an integer")

            if offset < old_offset or offset > size:
                raise ValueError("Checkpoint offset is out of range")

            # The caller must validate that the ACK matches the offset
            # actually sent on this connection.
            state["confirmed_offset"] = offset

            return offset

    def validate_resume(self, topic, filename, offset):
        """
        Validate a client-supplied checkpoint against the source file.

        This checks the topic, filename and byte range. It does not prove
        that the client has genuinely written the claimed bytes.
        """
        with self.lock:
            try:
                topic_dir = self._safe_topic_dir(topic)
            except ValueError:
                return False

            if not isinstance(filename, str) or not filename:
                return False

            if (
                filename in {".", ".."}
                or "/" in filename
                or "\\" in filename
            ):
                return False

            path = topic_dir / filename

            if not path.is_file() or path.is_symlink():
                return False

            size = path.stat().st_size

            if not isinstance(offset, int) or isinstance(offset, bool):
                return False

            if offset < 0 or offset > size:
                return False

            return True

    def reconstruct_checkpoint(self, topic, filename, offset):
        """
        Restore a checkpoint supplied by a reconnecting client.

        Call only after validate_resume() succeeds and after the previous
        worker has released the file.
        """
        with self.lock:
            if not self.validate_resume(topic, filename, offset):
                raise ValueError("Invalid resume checkpoint")

            key = (topic, filename)

            if topic not in self.topic_files:
                self.register_topic(topic)

            if key not in self.file_state:
                raise ValueError("File is not registered for this topic")

            state = self.file_state[key]

            if state["status"] == 1:
                raise ValueError("File is still owned by another worker")

            state["confirmed_offset"] = offset
            state["status"] = 0

    def mark_completed(self, topic, filename):
        """Mark a file complete after its full transfer is confirmed."""
        with self.lock:
            key = (topic, filename)

            if key not in self.file_state:
                raise ValueError("Unknown topic or filename")

            state = self.file_state[key]

            if state["status"] != 1:
                raise ValueError("File is not owned by a worker")

            if state["confirmed_offset"] != state["size"]:
                raise ValueError(
                    "Cannot complete a file before its full size is confirmed"
                )

            state["status"] = 2

    def release_failed_file(self, topic, filename):
        """
        Release a failed file for reassignment.

        Preserve the last confirmed offset.
        """
        with self.lock:
            key = (topic, filename)

            if key not in self.file_state:
                return

            state = self.file_state[key]

            if state["status"] == 1:
                state["status"] = 0

    def topic_completed(self, topic):
        """True only when every registered file has status 2."""
        with self.lock:
            if topic not in self.topic_files:
                return False

            return all(
                self.file_state[(topic, filename)]["status"] == 2
                for filename in self.topic_files[topic]
            )

    def snapshot(self, topic):
        """Return a consistent snapshot for debugging or metrics."""
        with self.lock:
            if topic not in self.topic_files:
                self.register_topic(topic)

            return {
                filename: dict(self.file_state[(topic, filename)])
                for filename in self.topic_files[topic]
            }
