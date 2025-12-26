from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from .http_client import ConfiguredHTTPJSONClient, HTTPJSONClient, HTTPRequestDefaults
from ..utils.utilities import (
    CacheIOTracker,
    RateLimiter,
)
from ..config import CACHE

WIKIMEDIA_PAGEVIEWS_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
USER_AGENT = "game-catalog-builder/1.0"


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
        base_http = HTTPJSONClient(self._session, stats=None)
        self.cache_path = Path(cache_path)
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)
        self._by_query: dict[str, Any] = {}
        # Built at load time for cheap fallback reuse of cached windows when offline. Keys:
        #   (project, access, article, days) -> (request_key, start_date, end_date)
        self._daily_index: dict[tuple[str, str, str, int], tuple[str, date, date]] = {}
        self.stats: dict[str, int] = {
            "by_query_hit": 0,
            "by_query_fetch": 0,
            "by_query_negative_hit": 0,
            "by_query_negative_fetch": 0,
            "by_query_fallback_hit": 0,
            # HTTP request counters (attempts, including retries).
            "http_get": 0,
        }
        base_http.stats = self.stats
        self._http = ConfiguredHTTPJSONClient(
            base_http,
            HTTPRequestDefaults(
                ratelimiter=self.ratelimiter,
                # Pageviews are an auxiliary signal; retrying heavily doesn't help much and makes
                # offline runs feel stuck. Keep retries low.
                retries=1,
                counter_key="http_get",
                headers={"User-Agent": USER_AGENT},
                status_handlers={404: {}},
                context_prefix="Wikipedia pageviews",
            ),
        )
        self._cache_io = CacheIOTracker(self.stats, min_interval_s=CACHE.save_min_interval_huge_s)
        self._load_cache(self._cache_io.load_json(self.cache_path))
        # If a run is offline and a cache miss requires network, disable further fetch attempts to
        # avoid spamming logs and spending time retrying.
        self._fetch_disabled = False
        self._fetch_disabled_logged = False

    def _parse_daily_request_key(self, request_key: str) -> tuple[str, str, str, date, date, int] | None:
        parts = str(request_key or "").split("|")
        if len(parts) < 7:
            return None
        project = parts[0]
        access = parts[1]
        agent = parts[2]
        granularity = parts[-3]
        start_s = parts[-2]
        end_s = parts[-1]
        if agent != "user" or granularity != "daily":
            return None
        article = "|".join(parts[3:-3])
        start = _parse_stamp_yyyymmdd00(start_s)
        end = _parse_stamp_yyyymmdd00(end_s)
        if start is None or end is None or end < start:
            return None
        days = (end - start).days + 1
        return project, access, article, start, end, days

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return
        by_query = raw.get("by_query")
        if isinstance(by_query, dict):
            self._by_query = {str(k): v for k, v in by_query.items()}
        self._daily_index = {}
        for key, payload in self._by_query.items():
            if payload == {}:
                continue
            parsed = self._parse_daily_request_key(key)
            if parsed is None:
                continue
            project, access, article, start, end, days = parsed
            idx_key = (project, access, article, days)
            existing = self._daily_index.get(idx_key)
            if existing is None or end > existing[2]:
                self._daily_index[idx_key] = (key, start, end)

    def _save_cache(self) -> None:
        self._cache_io.save_json({"by_query": self._by_query}, self.cache_path)

    def _get_cached(self, request_key: str, url: str) -> Any | None:
        cached = self._by_query.get(request_key)
        if cached is not None:
            self.stats["by_query_hit"] += 1
            if cached == {}:
                self.stats["by_query_negative_hit"] += 1
            return cached
        if self._fetch_disabled:
            return None
        try:
            data = self._http.get_json(
                url,
                context="",
                on_fail_return=None,
            )
        except RuntimeError as e:
            self._fetch_disabled = True
            if not self._fetch_disabled_logged:
                self._fetch_disabled_logged = True
                logging.error(
                    "[NETWORK] Wikipedia pageviews disabled for this run (cache-only). %s", e
                )
            return None
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
        if payload is None and self._fetch_disabled:
            cached = self._daily_index.get((project, access, article_norm, days))
            if cached is not None:
                cached_key, cached_start, cached_end = cached
                cached_payload = self._by_query.get(cached_key)
                if cached_payload not in (None, {}):
                    self.stats["by_query_fallback_hit"] += 1
                    daily = self._daily_views_for_range(
                        cached_payload, start=cached_start, end=cached_end
                    )
                    return daily[-days:] if len(daily) >= days else daily
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
            f"{HTTPJSONClient.format_timing(s, key='http_get')}, {CacheIOTracker.format_io(s)}"
        )
        fb = int(s.get("by_query_fallback_hit", 0) or 0)
        if fb:
            base = base + f", fallback_hit={fb}"
        http_429 = int(s.get("http_429", 0) or 0)
        if http_429:
            return (
                base
                + f", 429={http_429} retries={int(s.get('http_429_retries', 0) or 0)}"
                + f" backoff_ms={int(s.get('http_429_backoff_ms', 0) or 0)}"
            )
        return base
