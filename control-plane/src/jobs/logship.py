"""A job's log lines, sent to the control plane through its grant, so they outlive
its pods (``GET .../runs/{id}/logs``, ``GET .../uploads/{id}/logs``).

:class:`GrantLogHandler` batches the lines a pod logs and sends them every
``BATCH`` lines or ``EVERY_SECONDS``. It never fails the job: lines it can't send
are dropped (they're still in the pod's own log). Close it before the job reports
its outcome; the grant stops working then.
"""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime

from src.jobs.grant import GrantClient

BATCH = 200
EVERY_SECONDS = 2.0
#: The grant route takes at most this many lines a call.
PER_CALL = 1000
MAX_MESSAGE = 10_000


class GrantLogHandler(logging.Handler):
    def __init__(
        self, client: GrantClient, pod: str | None, every: float = EVERY_SECONDS
    ):
        super().__init__(level=logging.INFO)
        self.setFormatter(logging.Formatter("%(message)s"))
        self.client, self.pod = client, pod
        self._lines: list[dict] = []
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._stop = threading.Event()
        self._closed = False
        self._timer = threading.Thread(target=self._every, args=(every,), daemon=True)
        self._timer.start()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = {
                "at": datetime.fromtimestamp(record.created, UTC).isoformat(),
                "level": record.levelname,
                "message": self.format(record)[:MAX_MESSAGE],
            }
        except Exception:  # a line that can't be formatted isn't worth failing for
            return
        with self._lock:
            self._lines.append(line)
            full = len(self._lines) >= BATCH
        if full:
            self.flush()

    def flush(self) -> None:
        with self._send_lock:  # one send at a time keeps the lines in order
            with self._lock:
                lines, self._lines = self._lines, []
            for start in range(0, len(lines), PER_CALL):
                try:
                    self.client._grant(
                        "POST", "logs",
                        json={"pod": self.pod, "lines": lines[start:start + PER_CALL]},
                    )  # fmt: skip
                except Exception:
                    return  # dropped: the pod's own log still has them

    def _every(self, seconds: float) -> None:
        while not self._stop.wait(seconds):
            self.flush()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._stop.set()
            self.flush()
        super().close()


def ship_logs(client: GrantClient, pod: str | None) -> GrantLogHandler:
    """Send what the job logs (``src.jobs``) through its grant; returns the handler
    to close before the job reports its outcome."""
    handler = GrantLogHandler(client, pod)
    logger = logging.getLogger("src.jobs")
    if logger.getEffectiveLevel() > logging.INFO:
        logger.setLevel(logging.INFO)  # what a job's pod logs, whoever configured it
    logger.addHandler(handler)
    return handler
