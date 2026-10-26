"""Record of processed event_ids, shared by all workers through a plain text file.

A redelivered message can land on a different worker than the first delivery, so
each worker keeping its own in-memory set would miss it. Instead, every worker reads
and appends to data/processed_events.txt (one event_id per line). The final
check-and-record step runs under an OS file lock, so two workers holding copies of
the same event at the same moment cannot both apply it.
"""
import os
import time
from contextlib import contextmanager

if os.name == "nt":
    import msvcrt

    def _try_lock(f):
        f.seek(0)
        msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)

    def _unlock(f):
        f.seek(0)
        msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _try_lock(f):
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(f):
        fcntl.flock(f, fcntl.LOCK_UN)


class ProcessedEventStore:
    def __init__(self, data_dir):
        os.makedirs(data_dir, exist_ok=True)
        self.path = os.path.join(data_dir, "processed_events.txt")
        self.lock_path = os.path.join(data_dir, "processed_events.lock")

    def contains(self, event_id):
        try:
            with open(self.path, encoding="utf-8") as f:
                return any(line.strip() == event_id for line in f)
        except FileNotFoundError:
            return False

    def add(self, event_id):
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(event_id + "\n")
            f.flush()
            os.fsync(f.fileno())

    @contextmanager
    def lock(self):
        """Exclusive lock across processes. The OS releases it if the holder dies."""
        with open(self.lock_path, "a+b") as f:
            while True:
                try:
                    _try_lock(f)
                    break
                except OSError:
                    time.sleep(0.02)
            try:
                yield
            finally:
                _unlock(f)
