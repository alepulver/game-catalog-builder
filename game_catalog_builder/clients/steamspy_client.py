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
        self.cache: dict[str, Any] = load_json_cache(self.cache_path)
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)

    # -------------------------------------------------
    # Main query
    # -------------------------------------------------
    def fetch(self, appid: int) -> dict[str, Any] | None:
        key = str(appid)

        if key in self.cache:
            return self.cache[key]

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
            save_json_cache(self.cache, self.cache_path)
            return None

        # SteamSpy sometimes returns {"error": "..."}
        if "error" in data:
            logging.warning(f"Not found in SteamSpy: AppID {appid}. {data.get('error')}")
            self.cache[key] = None
            save_json_cache(self.cache, self.cache_path)
            return None

        extracted = {
            "SteamSpy_Owners": str(data.get("owners", "")),
            "SteamSpy_Players": str(data.get("players_forever", "")),
            "SteamSpy_CCU": str(data.get("ccu", "")),
            "SteamSpy_PlaytimeAvg": str(data.get("average_forever", "")),
        }

        self.cache[key] = extracted
        save_json_cache(self.cache, self.cache_path)
        return extracted
