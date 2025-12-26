from __future__ import annotations

import logging
import time
from dataclasses import dataclass


@dataclass
class Progress:
    label: str
    total: int | None
    every_n: int
    started_s: float = time.monotonic()

    def maybe_log(self, seen: int) -> None:
        if self.every_n <= 0:
            return
        if seen <= 0:
            return
        if seen % self.every_n != 0:
            return
        elapsed = time.monotonic() - self.started_s
        if self.total:
            logging.info(f"[{self.label}] Progress {seen}/{self.total} rows ({elapsed:.1f}s)")
        else:
            logging.info(f"[{self.label}] Progress {seen} rows ({elapsed:.1f}s)")

