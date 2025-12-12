from __future__ import annotations

import requests
from pathlib import Path
from typing import Dict, Any, Optional

from .utilities import (
    normalize_game_name,
    pick_best_match,
    load_json_cache,
    save_json_cache,
    RateLimiter,
    with_retries,
)

STEAM_SEARCH_URL = "https://store.steampowered.com/api/storesearch"
STEAM_APPDETAILS_URL = "https://store.steampowered.com/api/appdetails"


class SteamClient:
    def __init__(
        self,
        cache_path: str | Path,
        min_interval_s: float = 1.0,
    ):
        self.cache_path = Path(cache_path)
        self.cache: Dict[str, Any] = load_json_cache(self.cache_path)
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)

    # -------------------------------------------------
    # Search AppID by name
    # -------------------------------------------------
    def search_appid(self, game_name: str) -> Optional[Dict[str, Any]]:
        key = normalize_game_name(game_name)

        if key in self.cache:
            return self.cache[key]

        def _request():
            self.ratelimiter.wait()
            r = requests.get(
                STEAM_SEARCH_URL,
                params={
                    "term": game_name,
                    "l": "english",
                    "cc": "US",
                },
                timeout=10,
            )
            r.raise_for_status()
            return r.json()

        data = with_retries(_request, retries=3, on_fail_return=None)
        if not data or "items" not in data or not data["items"]:
            self.cache[key] = None
            save_json_cache(self.cache, self.cache_path)
            return None

        best, score = pick_best_match(game_name, data["items"], name_key="name")

        if not best or score < 65:
            self.cache[key] = None
            save_json_cache(self.cache, self.cache_path)
            return None

        self.cache[key] = best
        save_json_cache(self.cache, self.cache_path)
        return best

    # -------------------------------------------------
    # Game details
    # -------------------------------------------------
    def get_app_details(self, appid: int) -> Optional[Dict[str, Any]]:
        def _request():
            self.ratelimiter.wait()
            r = requests.get(
                STEAM_APPDETAILS_URL,
                params={"appids": appid, "l": "english"},
                timeout=10,
            )
            r.raise_for_status()
            return r.json()

        data = with_retries(_request, retries=3, on_fail_return=None)
        if not data or str(appid) not in data:
            return None

        entry = data[str(appid)]
        if not entry.get("success"):
            return None

        return entry.get("data")

    # -------------------------------------------------
    # Metadata extraction
    # -------------------------------------------------
    @staticmethod
    def extract_fields(appid: int, details: Dict[str, Any]) -> Dict[str, str]:
        if not details:
            return {}

        price = ""
        if details.get("is_free"):
            price = "Free"
        elif "price_overview" in details:
            price = str(details["price_overview"].get("final_formatted", ""))

        categories = [
            c.get("description", "")
            for c in details.get("categories", [])
        ]

        # Real Steam tags come indirectly from genres + categories + metadata
        genres = [g.get("description", "") for g in details.get("genres", [])]

        recommendations = details.get("recommendations", {})
        review_count = recommendations.get("total", "")

        return {
            "Steam_AppID": str(appid),
            "Steam_Tags": ", ".join(genres),
            "Steam_ReviewCount": str(review_count),
            "Steam_ReviewPercent": "",  # Steam doesn't expose this directly without extra scraping
            "Steam_Price": price,
            "Steam_Categories": ", ".join(categories),
        }
