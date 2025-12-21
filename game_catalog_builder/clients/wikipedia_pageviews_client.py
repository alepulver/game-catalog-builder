from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from ..config import REQUEST, RETRY
from ..utils.utilities import RateLimiter, load_json_cache, save_json_cache, with_retries

WIKIMEDIA_PAGEVIEWS_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
USER_AGENT = "game-catalog-builder/1.0 (contact: alepulver@protonmail.com)"


@dataclass(frozen=True)
class PageviewsSummary:
    days_30: int | None
    days_90: int | None
    days_365: int | None


def _stamp_yyyymmdd00(d: date) -> str:
    return d.strftime("%Y%m%d") + "00"


def _parse_yyyy_mm_dd(s: str) -> date | None:
    t = str(s or "").strip()
    if len(t) != 10 or t[4] != "-" or t[7] != "-":
        return None
    y, m, d = t[:4], t[5:7], t[8:10]
    if not (y.isdigit() and m.isdigit() and d.isdigit()):
        return None
    try:
        return date(int(y), int(m), int(d))
    except Exception:
        return None


class WikipediaPageviewsClient:
    """
    Wikipedia pageviews via Wikimedia REST API (official, no scraping).

    Cache format:
      - by_query: request_key -> raw response dict (or {} for not-found)

    Note: We cache the *actual request* (article+window+granularity). This keeps caches stable
    even if we change how we derive titles from user input.
    """

    def __init__(self, cache_path: str | Path, min_interval_s: float = 0.15):
        self.cache_path = Path(cache_path)
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)
        self._by_query: dict[str, Any] = {}
        self.stats: dict[str, int] = {
            "by_query_hit": 0,
            "by_query_fetch": 0,
            "by_query_negative_hit": 0,
            "by_query_negative_fetch": 0,
        }
        self._load_cache(load_json_cache(self.cache_path))

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return
        by_query = raw.get("by_query")
        if isinstance(by_query, dict):
            self._by_query = {str(k): v for k, v in by_query.items()}

    def _save_cache(self) -> None:
        save_json_cache({"by_query": self._by_query}, self.cache_path)

    def _get_cached(self, request_key: str, url: str) -> Any | None:
        cached = self._by_query.get(request_key)
        if cached is not None:
            self.stats["by_query_hit"] += 1
            if cached == {}:
                self.stats["by_query_negative_hit"] += 1
            return cached

        def _request():
            self.ratelimiter.wait()
            r = requests.get(
                url,
                timeout=REQUEST.timeout_s,
                headers={"User-Agent": USER_AGENT},
            )
            if r.status_code == 404:
                return {}
            r.raise_for_status()
            return r.json()

        data = with_retries(
            _request,
            retries=RETRY.retries,
            on_fail_return=None,
            context="Wikipedia pageviews",
        )
        if data is None:
            logging.warning(
                "Wikipedia pageviews request failed (no response); not caching as not-found."
            )
            return None

        self._by_query[request_key] = data
        self._save_cache()
        self.stats["by_query_fetch"] += 1
        if data == {}:
            self.stats["by_query_negative_fetch"] += 1
        return data

    def _sorted_daily_views(self, payload: Any) -> list[int]:
        if not isinstance(payload, dict):
            return []
        items = payload.get("items")
        if not isinstance(items, list):
            return []
        pairs: list[tuple[str, int]] = []
        for it in items:
            if not isinstance(it, dict):
                continue
            v = it.get("views")
            ts = str(it.get("timestamp") or "")
            if isinstance(v, int) and v >= 0 and ts:
                pairs.append((ts, v))
        pairs.sort(key=lambda x: x[0])
        return [v for _, v in pairs]

    def get_pageviews_daily_series(
        self, *, project: str, article: str, days: int, access: str = "all-access"
    ) -> list[int]:
        """
        Return daily pageviews over the last N days (ending yesterday), sorted oldest->newest.

        Caches the raw response keyed by the actual request.
        """
        article_norm = str(article or "").strip()
        if not article_norm:
            return []
        if days <= 0:
            return []

        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        start_s = _stamp_yyyymmdd00(start)
        end_s = _stamp_yyyymmdd00(end)

        article_enc = quote(article_norm, safe="")
        url = (
            f"{WIKIMEDIA_PAGEVIEWS_API}/"
            f"{project}/{access}/user/{article_enc}/daily/{start_s}/{end_s}"
        )
        request_key = f"{project}|{access}|user|{article_norm}|daily|{start_s}|{end_s}"
        payload = self._get_cached(request_key, url)
        if payload is None or payload == {}:
            return []
        return self._sorted_daily_views(payload)

    def get_pageviews_daily_series_range(
        self,
        *,
        project: str,
        article: str,
        start: date,
        end: date,
        access: str = "all-access",
    ) -> list[int]:
        """
        Return daily pageviews for an explicit date range (inclusive), sorted oldest->newest.

        Used for "launch interest" proxies like first 30/90 days since release.
        """
        article_norm = str(article or "").strip()
        if not article_norm:
            return []
        if end < start:
            return []

        start_s = _stamp_yyyymmdd00(start)
        end_s = _stamp_yyyymmdd00(end)

        article_enc = quote(article_norm, safe="")
        url = (
            f"{WIKIMEDIA_PAGEVIEWS_API}/"
            f"{project}/{access}/user/{article_enc}/daily/{start_s}/{end_s}"
        )
        request_key = f"{project}|{access}|user|{article_norm}|daily|{start_s}|{end_s}"
        payload = self._get_cached(request_key, url)
        if payload is None or payload == {}:
            return []
        return self._sorted_daily_views(payload)

    def get_pageviews_summary_enwiki(self, enwiki_title: str) -> PageviewsSummary:
        title = str(enwiki_title or "").strip()
        if not title:
            return PageviewsSummary(None, None, None)

        # Fetch a single 365-day daily series and derive the other windows locally to avoid
        # multiple API calls per title.
        daily = self.get_pageviews_daily_series(
            project="en.wikipedia.org",
            article=title,
            days=365,
        )
        if not daily:
            return PageviewsSummary(None, None, None)

        p365 = sum(daily) if daily else None
        p90 = sum(daily[-90:]) if len(daily) >= 90 else sum(daily)
        p30 = sum(daily[-30:]) if len(daily) >= 30 else sum(daily)
        return PageviewsSummary(p30, p90, p365)

    def get_pageviews_first_days_since_release_enwiki(
        self,
        *,
        enwiki_title: str,
        release_date: str,
        days: int = 90,
        earliest_supported: date = date(2015, 7, 1),
    ) -> int | None:
        """
        Sum pageviews for the first N days since release (inclusive), when feasible.

        Note: Wikimedia Pageviews data is available starting mid-2015; for older releases,
        this returns None.
        """
        title = str(enwiki_title or "").strip()
        if not title:
            return None
        d0 = _parse_yyyy_mm_dd(release_date)
        if d0 is None:
            return None

        end_yesterday = date.today() - timedelta(days=1)
        if d0 < earliest_supported:
            return None
        if d0 > end_yesterday:
            return None
        if days <= 0:
            return None

        end = d0 + timedelta(days=days - 1)
        if end > end_yesterday:
            end = end_yesterday
        daily = self.get_pageviews_daily_series_range(
            project="en.wikipedia.org", article=title, start=d0, end=end
        )
        if not daily:
            return None
        return int(sum(daily))

    def format_cache_stats(self) -> str:
        s = self.stats
        return (
            f"by_query hit={s['by_query_hit']} fetch={s['by_query_fetch']} "
            f"(neg hit={s['by_query_negative_hit']} fetch={s['by_query_negative_fetch']})"
        )
