from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import requests

from ..utils.utilities import (
    RateLimiter,
    load_json_cache,
    save_json_cache,
    with_retries,
)

STEAMSPY_URL = "https://steamspy.com/api.php"


class SteamSpyClient:
    def __init__(
        self,
        cache_path: str | Path,
        min_interval_s: float = 1.0,
    ):
        self.cache_path = Path(cache_path)
        self.stats: dict[str, int] = {
            "by_id_hit": 0,
            "by_id_fetch": 0,
            "by_id_negative_hit": 0,
            "by_id_negative_fetch": 0,
        }
        raw = load_json_cache(self.cache_path)
        if isinstance(raw, dict) and isinstance(raw.get("by_id"), dict):
            self.cache = raw.get("by_id") or {}
        elif not raw:
            self.cache = {}
        else:
            logging.warning(
                "SteamSpy cache file is in an incompatible format; ignoring it (delete it to rebuild)."
            )
            self.cache = {}
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)

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
            r = requests.get(
                STEAMSPY_URL,
                params={
                    "request": "appdetails",
                    "appid": appid,
                },
                timeout=10,
            )
            r.raise_for_status()
            return r.json()

        data = with_retries(_request, retries=3, on_fail_return=None)
        if not data or not isinstance(data, dict):
            self.cache[key] = None
            save_json_cache({"by_id": self.cache}, self.cache_path)
            self.stats["by_id_negative_fetch"] += 1
            return None

        # SteamSpy sometimes returns {"error": "..."}
        if "error" in data:
            logging.warning(f"Not found in SteamSpy: AppID {appid}. {data.get('error')}")
            self.cache[key] = None
            save_json_cache({"by_id": self.cache}, self.cache_path)
            self.stats["by_id_negative_fetch"] += 1
            return None

        # Cache the full provider response; extraction is computed on-demand.
        self.cache[key] = data
        save_json_cache({"by_id": self.cache}, self.cache_path)
        self.stats["by_id_fetch"] += 1
        return self._extract_fields(data)

    @staticmethod
    def _extract_fields(data: dict[str, Any]) -> dict[str, str]:
        return {
            "SteamSpy_Owners": str(data.get("owners", "")),
            "SteamSpy_Players": str(data.get("players_forever", "")),
            "SteamSpy_CCU": str(data.get("ccu", "")),
            "SteamSpy_PlaytimeAvg": str(data.get("average_forever", "")),
        }

    def format_cache_stats(self) -> str:
        s = self.stats
        return (
            f"by_id hit={s['by_id_hit']} fetch={s['by_id_fetch']} "
            f"(neg hit={s['by_id_negative_hit']} fetch={s['by_id_negative_fetch']})"
        )
