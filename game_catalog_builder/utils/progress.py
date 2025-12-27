from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from ..config import CLI


@dataclass
class Progress:
    label: str
    total: int | None
    every_n: int
    started_s: float = time.monotonic()
    last_log_s: float = time.monotonic()
    last_seen: int = 0

    def maybe_log(self, seen: int) -> None:
        if seen <= 0:
            return

        now = time.monotonic()
        should_log = False

        min_interval = float(getattr(CLI, "progress_min_interval_s", 0.0) or 0.0)
        if min_interval > 0:
            # Prefer time-based progress so long runs never look "stuck" in logs.
            # Still log the final line promptly at completion.
            if (now - self.last_log_s) >= min_interval and seen != self.last_seen:
                should_log = True
            if self.total and seen >= self.total and seen != self.last_seen:
                should_log = True
        elif self.every_n > 0 and seen % self.every_n == 0:
            should_log = True

        if not should_log:
            return

        elapsed = time.monotonic() - self.started_s
        self.last_log_s = now
        self.last_seen = seen
        if self.total:
            logging.info(f"[{self.label}] Progress {seen}/{self.total} rows ({elapsed:.1f}s)")
        else:
            logging.info(f"[{self.label}] Progress {seen} rows ({elapsed:.1f}s)")
