from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable


@dataclass
class EveryN:
    """
    Invoke a callback every N items (typically rows), plus an optional time-based guard.
    """

    every_n: int
    callback: Callable[[], None]
    min_interval_s: float = 0.0
    _last_s: float = 0.0

    def maybe(self, count: int) -> None:
        if self.every_n <= 0:
            return
        if count <= 0 or (count % self.every_n) != 0:
            return
        now = time.monotonic()
        if self.min_interval_s > 0 and self._last_s and (now - self._last_s) < self.min_interval_s:
            return
        self.callback()
        self._last_s = now

