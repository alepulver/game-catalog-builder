from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import requests

from ..config import REQUEST, RETRY, STEAMSPY
from ..utils.utilities import (
    CacheIOTracker,
    RateLimiter,
    network_failures_count,
    raise_on_new_network_failure,
    with_retries,
)

STEAMSPY_URL = "https://steamspy.com/api.php"


class SteamSpyClient:
    def __init__(
        self,
        cache_path: str | Path,
        min_interval_s: float = STEAMSPY.min_interval_s,
    ):
        self._session = requests.Session()
        self.cache_path = Path(cache_path)
        self.stats: dict[str, int] = {
            "by_id_hit": 0,
            "by_id_fetch": 0,
            "by_id_negative_hit": 0,
            "by_id_negative_fetch": 0,
            # HTTP request counters (attempts, including retries).
            "http_get": 0,
        }
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

        def _request():
            self.ratelimiter.wait()
            self.stats["http_get"] += 1
            r = self._session.get(
                STEAMSPY_URL,
                params={
                    "request": "appdetails",
                    "appid": appid,
                },
                timeout=REQUEST.timeout_s,
            )
            r.raise_for_status()
            return r.json()

        before_net = network_failures_count(self.stats)
        data = with_retries(
            _request, retries=RETRY.retries, on_fail_return=None, retry_stats=self.stats
        )
        if data is None:
            raise_on_new_network_failure(
                self.stats, before=before_net, context=f"SteamSpy appdetails appid={appid}"
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
        try:
            pos_i = int(str(positive))
            neg_i = int(str(negative))
            denom = pos_i + neg_i
            if denom > 0:
                score_100 = str(int(round(pos_i / denom * 100.0)))
        except Exception:
            score_100 = ""
        return {
            "SteamSpy_Owners": str(data.get("owners", "")),
            "SteamSpy_Players": str(data.get("players_forever", "")),
            "SteamSpy_Players2Weeks": str(data.get("players_2weeks", "")),
            "SteamSpy_CCU": str(data.get("ccu", "")),
            "SteamSpy_PlaytimeAvg": str(data.get("average_forever", "")),
            "SteamSpy_PlaytimeAvg2Weeks": str(data.get("average_2weeks", "")),
            "SteamSpy_PlaytimeMedian2Weeks": str(data.get("median_2weeks", "")),
            "SteamSpy_Positive": str(positive),
            "SteamSpy_Negative": str(negative),
            "Score_SteamSpy_100": score_100,
        }

    def format_cache_stats(self) -> str:
        s = self.stats
        base = (
            f"by_id hit={s['by_id_hit']} fetch={s['by_id_fetch']} "
            f"(neg hit={s['by_id_negative_hit']} fetch={s['by_id_negative_fetch']}), "
            f"http get={s['http_get']}"
        )
        base += (
            f", cache load_ms={int(s.get('cache_load_ms', 0) or 0)}"
            f" saves={int(s.get('cache_save_count', 0) or 0)}"
            f" save_ms={int(s.get('cache_save_ms', 0) or 0)}"
        )
        http_429 = int(s.get("http_429", 0) or 0)
        if http_429:
            return (
                base
                + f", 429={http_429} retries={int(s.get('http_429_retries', 0) or 0)}"
                + f" backoff_ms={int(s.get('http_429_backoff_ms', 0) or 0)}"
            )
        return base
