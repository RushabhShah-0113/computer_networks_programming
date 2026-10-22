
import threading
from session import SessionManager


class FileScheduler:
    """
    Thread-safe dynamic file scheduler.

    File states:
        0 = available
        1 = transmitting
        2 = completed

    A worker claims one file, transfers it, then claims another.
    """

    def __init__(self, session_manager):
        if not isinstance(session_manager, SessionManager):
            raise TypeError("Expected a SessionManager instance")

        self.sessions = session_manager
        self.lock = threading.Lock()

        # Track which worker owns each active file.
        # (topic, filename) -> worker_id
        self.owners = {}

    def initialize_topic(self, topic):
        """Register the topic and initialize its file states."""
        return self.sessions.register_topic(topic)

    def get_next_file(self, topic, worker_id):
        """
        Claim the next available file.

        Returns:
            {
                "topic": topic,
                "filename": filename,
                "size": file_size,
                "offset": confirmed_offset,
                "worker_id": worker_id
            }

        Returns None when no file is currently available.
        """
        with self.lock:
            filenames = self.sessions.get_topic_files(topic)

            for filename in filenames:
                key = (topic, filename)
                state = self.sessions.get_file_state(topic, filename)

                if state["status"] != 0:
                    continue

                offset = self.sessions.claim_file(topic, filename)

                if offset is None:
                    continue

                self.owners[key] = worker_id

                return {
                    "topic": topic,
                    "filename": filename,
                    "size": state["size"],
                    "offset": offset,
                    "worker_id": worker_id,
                }

            return None

    def mark_completed(self, topic, filename, worker_id):
        """
        Mark a file completed after all bytes are checkpoint-confirmed.
        """
        with self.lock:
            key = (topic, filename)

            if self.owners.get(key) != worker_id:
                raise RuntimeError(
                    "Worker does not own this file"
                )

            self.sessions.mark_completed(topic, filename)
            del self.owners[key]

    def release_failed_file(self, topic, filename, worker_id):
        """
        Release a failed assignment while preserving its checkpoint.
        """
        with self.lock:
            key = (topic, filename)

            if self.owners.get(key) != worker_id:
                # Avoid releasing another worker's assignment.
                return False

            self.sessions.release_failed_file(topic, filename)
            del self.owners[key]

            return True

    def topic_completed(self, topic):
        """Check whether every file in the topic is complete."""
        return self.sessions.topic_completed(topic)

    def get_topic_snapshot(self, topic):
        """Return file states for debugging and progress reporting."""
        return self.sessions.snapshot(topic)
