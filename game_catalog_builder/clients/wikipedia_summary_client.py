from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from ..config import REQUEST, RETRY
from ..utils.utilities import (
    CacheIOTracker,
    RateLimiter,
    network_failures_count,
    raise_on_new_network_failure,
    with_retries,
)

WIKIPEDIA_SUMMARY_API = "https://en.wikipedia.org/api/rest_v1/page/summary"
USER_AGENT = "game-catalog-builder/1.0 (contact: alepulver@protonmail.com)"


class WikipediaSummaryClient:
    """
    Wikipedia page summaries via Wikimedia REST API (official, no scraping).

    Cache format:
      - by_title: enwiki_title -> raw response dict (or {} for not-found)
    """

    def __init__(self, cache_path: str | Path, min_interval_s: float = 0.15):
        self._session = requests.Session()
        self.cache_path = Path(cache_path)
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)
        self._by_title: dict[str, Any] = {}
        self.stats: dict[str, int] = {
            "by_title_hit": 0,
            "by_title_fetch": 0,
            "by_title_negative_hit": 0,
            "by_title_negative_fetch": 0,
            # HTTP request counters (attempts, including retries).
            "http_get": 0,
        }
        self._cache_io = CacheIOTracker(self.stats)
        self._load_cache(self._cache_io.load_json(self.cache_path))

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return
        by_title = raw.get("by_title")
        if isinstance(by_title, dict):
            self._by_title = {str(k): v for k, v in by_title.items()}

    def _save_cache(self) -> None:
        self._cache_io.save_json({"by_title": self._by_title}, self.cache_path)

    def get_summary(self, enwiki_title: str) -> dict[str, Any] | None:
        title = str(enwiki_title or "").strip()
        if not title:
            return None

        cached = self._by_title.get(title)
        if cached is not None:
            self.stats["by_title_hit"] += 1
            if cached == {}:
                self.stats["by_title_negative_hit"] += 1
            return cached

        def _request():
            self.ratelimiter.wait()
            self.stats["http_get"] += 1
            url = f"{WIKIPEDIA_SUMMARY_API}/{quote(title, safe='')}"
            r = self._session.get(url, timeout=REQUEST.timeout_s, headers={"User-Agent": USER_AGENT})
            if r.status_code == 404:
                return {}
            r.raise_for_status()
            return r.json()

        before_net = network_failures_count(self.stats)
        data = with_retries(
            _request,
            retries=RETRY.retries,
            on_fail_return=None,
            context="Wikipedia summary",
            retry_stats=self.stats,
        )
        if data is None:
            logging.warning(
                "Wikipedia summary request failed (no response); not caching as not-found."
            )
            raise_on_new_network_failure(self.stats, before=before_net, context="Wikipedia summary")
            return None

        self._by_title[title] = data
        self._save_cache()
        self.stats["by_title_fetch"] += 1
        if data == {}:
            self.stats["by_title_negative_fetch"] += 1
        return data

    def format_cache_stats(self) -> str:
        s = self.stats
        base = (
            f"by_title hit={s['by_title_hit']} fetch={s['by_title_fetch']} "
            f"(neg hit={s['by_title_negative_hit']} fetch={s['by_title_negative_fetch']}), "
            f"http get={s['http_get']}, "
            f"cache load_ms={int(s.get('cache_load_ms', 0) or 0)} "
            f"saves={int(s.get('cache_save_count', 0) or 0)} "
            f"save_ms={int(s.get('cache_save_ms', 0) or 0)}"
        )
        http_429 = int(s.get("http_429", 0) or 0)
        if http_429:
            return (
                base
                + f", 429={http_429} retries={int(s.get('http_429_retries', 0) or 0)}"
                + f" backoff_ms={int(s.get('http_429_backoff_ms', 0) or 0)}"
            )
        return base
