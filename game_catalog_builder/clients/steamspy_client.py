from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import requests

from ..config import RETRY, STEAMSPY
from .http_client import ConfiguredHTTPJSONClient, HTTPJSONClient, HTTPRequestDefaults
from ..utils.utilities import (
    CacheIOTracker,
    RateLimiter,
)

STEAMSPY_URL = "https://steamspy.com/api.php"


class SteamSpyClient:
    def __init__(
        self,
        cache_path: str | Path,
        min_interval_s: float = STEAMSPY.min_interval_s,
    ):
        self._session = requests.Session()
        base_http = HTTPJSONClient(self._session, stats=None)
        self.cache_path = Path(cache_path)
        self.stats: dict[str, int] = {
            "by_id_hit": 0,
            "by_id_fetch": 0,
            "by_id_negative_hit": 0,
            "by_id_negative_fetch": 0,
            # HTTP request counters (attempts, including retries).
            "http_get": 0,
        }
        base_http.stats = self.stats
        self._cache_io = CacheIOTracker(self.stats)
        raw = self._cache_io.load_json(self.cache_path)
        if isinstance(raw, dict) and isinstance(raw.get("by_id"), dict):
            self.cache = raw.get("by_id") or {}
        elif not raw:
            self.cache = {}
        else:
            logging.warning(
                "SteamSpy cache file is in an incompatible format; ignoring it "
                "(delete it to rebuild)."
            )
            self.cache = {}
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)
        self._http = ConfiguredHTTPJSONClient(
            base_http,
            HTTPRequestDefaults(
                ratelimiter=self.ratelimiter,
                retries=RETRY.retries,
                counter_key="http_get",
                context_prefix="SteamSpy",
            ),
        )

    def _save_cache(self) -> None:
        self._cache_io.save_json({"by_id": self.cache}, self.cache_path)

    # -------------------------------------------------
    # Main query
    # -------------------------------------------------
    def fetch(self, appid: int) -> dict[str, Any] | None:
        key = str(appid)

        if key in self.cache:
            cached = self.cache[key]
            if not isinstance(cached, dict):
                self.stats["by_id_negative_hit"] += 1
                return None
            self.stats["by_id_hit"] += 1
            return self._extract_fields(cached)
        data = self._http.get_json(
            STEAMSPY_URL,
            params={
                "request": "appdetails",
                "appid": appid,
            },
            context=f"appdetails appid={appid}",
            on_fail_return=None,
        )
        if not data or not isinstance(data, dict):
            self.cache[key] = None
            self._save_cache()
            self.stats["by_id_negative_fetch"] += 1
            return None

        # SteamSpy sometimes returns {"error": "..."}
        if "error" in data:
            logging.warning(f"Not found in SteamSpy: AppID {appid}. {data.get('error')}")
            self.cache[key] = None
            self._save_cache()
            self.stats["by_id_negative_fetch"] += 1
            return None

        # Cache the full provider response; extraction is computed on-demand.
        self.cache[key] = data
        self._save_cache()
        self.stats["by_id_fetch"] += 1
        return self._extract_fields(data)

    @staticmethod
    def _extract_fields(data: dict[str, Any]) -> dict[str, str]:
        positive = data.get("positive", "")
        negative = data.get("negative", "")
        score_100 = ""
        rate = ""
        try:
            pos_i = int(str(positive))
            neg_i = int(str(negative))
            denom = pos_i + neg_i
            if denom > 0:
                rate_f = pos_i / denom
                rate = f"{rate_f:.4f}"
                score_100 = str(int(round(rate_f * 100.0)))
        except Exception:
            score_100 = ""
            rate = ""
        return {
            "SteamSpy_Owners": str(data.get("owners", "")),
            "SteamSpy_CCU": str(data.get("ccu", "")),
            "SteamSpy_PlaytimeAvg": str(data.get("average_forever", "")),
            "SteamSpy_PlaytimeAvg2Weeks": str(data.get("average_2weeks", "")),
            "SteamSpy_PlaytimeMedian2Weeks": str(data.get("median_2weeks", "")),
            "SteamSpy_Positive": str(positive),
            "SteamSpy_Negative": str(negative),
            "SteamSpy_PositiveRate": rate,
            "Score_SteamSpy_100": score_100,
        }

    def format_cache_stats(self) -> str:
        s = self.stats
        base = (
            f"by_id hit={s['by_id_hit']} fetch={s['by_id_fetch']} "
            f"(neg hit={s['by_id_negative_hit']} fetch={s['by_id_negative_fetch']}), "
            f"{HTTPJSONClient.format_timing(s, key='http_get')}"
        )
        base += f", {CacheIOTracker.format_io(s)}"
        http_429 = int(s.get("http_429", 0) or 0)
        if http_429:
            return (
                base
                + f", 429={http_429} retries={int(s.get('http_429_retries', 0) or 0)}"
                + f" backoff_ms={int(s.get('http_429_backoff_ms', 0) or 0)}"
            )
        return base
