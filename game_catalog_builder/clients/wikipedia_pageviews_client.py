from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
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
from ..config import CACHE

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


def _parse_stamp_yyyymmdd00(s: str) -> date | None:
    t = str(s or "").strip()
    if len(t) != 10 or not t.endswith("00"):
        return None
    y = t[0:4]
    m = t[4:6]
    d = t[6:8]
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
        self._session = requests.Session()
        self.cache_path = Path(cache_path)
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)
        self._by_query: dict[str, Any] = {}
        self.stats: dict[str, int] = {
            "by_query_hit": 0,
            "by_query_fetch": 0,
            "by_query_negative_hit": 0,
            "by_query_negative_fetch": 0,
            # HTTP request counters (attempts, including retries).
            "http_get": 0,
        }
        self._cache_io = CacheIOTracker(self.stats, min_interval_s=CACHE.save_min_interval_large_s)
        self._load_cache(self._cache_io.load_json(self.cache_path))

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return
        by_query = raw.get("by_query")
        if isinstance(by_query, dict):
            self._by_query = {str(k): v for k, v in by_query.items()}

    def _save_cache(self) -> None:
        self._cache_io.save_json({"by_query": self._by_query}, self.cache_path)

    def _get_cached(self, request_key: str, url: str) -> Any | None:
        cached = self._by_query.get(request_key)
        if cached is not None:
            self.stats["by_query_hit"] += 1
            if cached == {}:
                self.stats["by_query_negative_hit"] += 1
            return cached

        def _request():
            self.ratelimiter.wait()
            self.stats["http_get"] += 1
            r = self._session.get(
                url,
                timeout=REQUEST.timeout_s,
                headers={"User-Agent": USER_AGENT},
            )
            if r.status_code == 404:
                return {}
            r.raise_for_status()
            return r.json()

        before_net = network_failures_count(self.stats)
        data = with_retries(
            _request,
            retries=RETRY.retries,
            on_fail_return=None,
            context="Wikipedia pageviews",
            retry_stats=self.stats,
        )
        if data is None:
            logging.warning(
                "Wikipedia pageviews request failed (no response); not caching as not-found."
            )
            raise_on_new_network_failure(self.stats, before=before_net, context="Wikipedia pageviews")
            return None

        self._by_query[request_key] = data
        self._save_cache()
        self.stats["by_query_fetch"] += 1
        if data == {}:
            self.stats["by_query_negative_fetch"] += 1
        return data

    def _daily_views_for_range(self, payload: Any, *, start: date, end: date) -> list[int]:
        if not isinstance(payload, dict):
            return []
        items = payload.get("items")
        if not isinstance(items, list):
            return []
        by_day: dict[date, int] = {}
        for it in items:
            if not isinstance(it, dict):
                continue
            v = it.get("views")
            ts = str(it.get("timestamp") or "").strip()
            d = _parse_stamp_yyyymmdd00(ts) if ts else None
            if d is None:
                continue
            if isinstance(v, int) and v >= 0:
                by_day[d] = v

        out: list[int] = []
        cur = start
        while cur <= end:
            out.append(int(by_day.get(cur, 0) or 0))
            cur = cur + timedelta(days=1)
        return out

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
        return self._daily_views_for_range(payload, start=start, end=end)

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
        return self._daily_views_for_range(payload, start=start, end=end)

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

    def get_pageviews_launch_summary_enwiki(
        self,
        *,
        enwiki_title: str,
        release_date: str,
        earliest_supported: date = date(2015, 7, 1),
    ) -> PageviewsSummary:
        """
        Return the first-30 and first-90 day pageviews since release, when feasible.

        Uses at most one HTTP request:
          - If the release date is within the last 365 days, reuses the cached 365-day series.
          - Otherwise fetches a single explicit range [release_date, release_date+89] (capped).
        """
        title = str(enwiki_title or "").strip()
        if not title:
            return PageviewsSummary(None, None, None)
        d0 = _parse_yyyy_mm_dd(release_date)
        if d0 is None:
            return PageviewsSummary(None, None, None)

        end_yesterday = date.today() - timedelta(days=1)
        if d0 < earliest_supported:
            return PageviewsSummary(None, None, None)
        if d0 > end_yesterday:
            return PageviewsSummary(None, None, None)

        last_start = end_yesterday - timedelta(days=365 - 1)
        if d0 >= last_start:
            # Reuse the cached 365-day series to avoid extra API calls for recent releases.
            daily = self.get_pageviews_daily_series(
                project="en.wikipedia.org",
                article=title,
                days=365,
            )
            if not daily:
                return PageviewsSummary(None, None, None)
            offset = (d0 - last_start).days
            if offset < 0 or offset >= len(daily):
                return PageviewsSummary(None, None, None)
            since = daily[offset:]
            if not since:
                return PageviewsSummary(None, None, None)
            first90 = int(sum(since[:90]))
            first30 = int(sum(since[:30]))
            return PageviewsSummary(first30, first90, None)

        # Older releases (but within Wikimedia coverage): fetch a single 90-day range and derive
        # first-30 locally.
        end = d0 + timedelta(days=90 - 1)
        if end > end_yesterday:
            end = end_yesterday
        daily = self.get_pageviews_daily_series_range(
            project="en.wikipedia.org", article=title, start=d0, end=end
        )
        if not daily:
            return PageviewsSummary(None, None, None)
        first90 = int(sum(daily))
        first30 = int(sum(daily[:30])) if len(daily) >= 30 else int(sum(daily))
        return PageviewsSummary(first30, first90, None)

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
        if days <= 0:
            return None
        summary = self.get_pageviews_launch_summary_enwiki(
            enwiki_title=enwiki_title, release_date=release_date, earliest_supported=earliest_supported
        )
        if days <= 30:
            return summary.days_30
        if days <= 90:
            return summary.days_90
        # For now, only expose first-30/first-90; larger windows would need a larger range.
        return None

    def format_cache_stats(self) -> str:
        s = self.stats
        base = (
            f"by_query hit={s['by_query_hit']} fetch={s['by_query_fetch']} "
            f"(neg hit={s['by_query_negative_hit']} fetch={s['by_query_negative_fetch']}), "
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
