from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from ..config import REQUEST, RETRY
from ..utils.utilities import RateLimiter, load_json_cache, save_json_cache, with_retries

WIKIPEDIA_SUMMARY_API = "https://en.wikipedia.org/api/rest_v1/page/summary"
USER_AGENT = "game-catalog-builder/1.0 (contact: alepulver@protonmail.com)"


class WikipediaSummaryClient:
    """
    Wikipedia page summaries via Wikimedia REST API (official, no scraping).

    Cache format:
      - by_title: enwiki_title -> raw response dict (or {} for not-found)
    """

    def __init__(self, cache_path: str | Path, min_interval_s: float = 0.15):
        self.cache_path = Path(cache_path)
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)
        self._by_title: dict[str, Any] = {}
        self.stats: dict[str, int] = {
            "by_title_hit": 0,
            "by_title_fetch": 0,
            "by_title_negative_hit": 0,
            "by_title_negative_fetch": 0,
        }
        self._load_cache(load_json_cache(self.cache_path))

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return
        by_title = raw.get("by_title")
        if isinstance(by_title, dict):
            self._by_title = {str(k): v for k, v in by_title.items()}

    def _save_cache(self) -> None:
        save_json_cache({"by_title": self._by_title}, self.cache_path)

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
            url = f"{WIKIPEDIA_SUMMARY_API}/{quote(title, safe='')}"
            r = requests.get(url, timeout=REQUEST.timeout_s, headers={"User-Agent": USER_AGENT})
            if r.status_code == 404:
                return {}
            r.raise_for_status()
            return r.json()

        data = with_retries(
            _request,
            retries=RETRY.retries,
            on_fail_return=None,
            context="Wikipedia summary",
        )
        if data is None:
            logging.warning(
                "Wikipedia summary request failed (no response); not caching as not-found."
            )
            return None

        self._by_title[title] = data
        self._save_cache()
        self.stats["by_title_fetch"] += 1
        if data == {}:
            self.stats["by_title_negative_fetch"] += 1
        return data

    def format_cache_stats(self) -> str:
        s = self.stats
        return (
            f"by_title hit={s['by_title_hit']} fetch={s['by_title_fetch']} "
            f"(neg hit={s['by_title_negative_hit']} fetch={s['by_title_negative_fetch']})"
        )
