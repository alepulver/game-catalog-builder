from __future__ import annotations

import logging
import requests
from pathlib import Path
from typing import Dict, Any, Optional

from ..utils.utilities import (
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
        self._by_id: Dict[str, Any] = {}
        self._by_name: Dict[str, Optional[str]] = {}
        self._details_by_id: Dict[str, Any] = {}
        self._load_cache(load_json_cache(self.cache_path))
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return

        by_id = raw.get("by_id")
        by_name = raw.get("by_name")
        by_details = raw.get("by_details")
        if isinstance(by_id, dict) and isinstance(by_name, dict):
            self._by_id = {str(k): v for k, v in by_id.items()}
            self._by_name = {str(k): (str(v) if v else None) for k, v in by_name.items()}
            if isinstance(by_details, dict):
                self._details_by_id = {str(k): v for k, v in by_details.items()}
            return

        # Backward compatibility: old caches stored name_key -> best item / None.
        for name_key, value in raw.items():
            if value is None:
                self._by_name[str(name_key)] = None
                continue
            if not isinstance(value, dict):
                continue
            appid = value.get("id")
            if appid is None:
                continue
            appid_str = str(appid)
            self._by_id[appid_str] = value
            self._by_name[str(name_key)] = appid_str

    def _save_cache(self) -> None:
        save_json_cache(
            {"by_id": self._by_id, "by_name": self._by_name, "by_details": self._details_by_id},
            self.cache_path,
        )

    # -------------------------------------------------
    # Search AppID by name
    # -------------------------------------------------
    def search_appid(self, game_name: str) -> Optional[Dict[str, Any]]:
        key = normalize_game_name(game_name)

        if key in self._by_name:
            appid = self._by_name[key]
            if not appid:
                return None
            return self._by_id.get(str(appid))

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
            # No results from API - log warning
            logging.warning(f"Not found on Steam: '{game_name}'. No results from API.")
            self._by_name[key] = None
            self._save_cache()
            return None

        best, score, top_matches = pick_best_match(game_name, data["items"], name_key="name")

        if not best or score < 65:
            # Log top 5 closest matches when not found
            if top_matches:
                top_names = [f"'{name}' ({s}%)" for name, s in top_matches[:5]]
                logging.warning(
                    f"Not found on Steam: '{game_name}'. Closest matches: {', '.join(top_names)}"
                )
            else:
                logging.warning(f"Not found on Steam: '{game_name}'. No matches found.")
            self._by_name[key] = None
            self._save_cache()
            return None

        # Warn if there are close matches (but not if it's a perfect 100% match)
        if top_matches and score < 100:
            top_names = [f"'{name}' ({s}%)" for name, s in top_matches[:5]]
            logging.warning(
                f"Close match for '{game_name}': Selected '{best.get('name', '')}' (score: {score}%), "
                f"alternatives: {', '.join(top_names)}"
            )

        appid = best.get("id")
        if appid is not None:
            appid_str = str(appid)
            self._by_id[appid_str] = best
            self._by_name[key] = appid_str
            self._save_cache()
        return best

    # -------------------------------------------------
    # Game details
    # -------------------------------------------------
    def get_app_details(self, appid: int) -> Optional[Dict[str, Any]]:
        cached = self._details_by_id.get(str(appid))
        if isinstance(cached, dict):
            return cached

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

        details = entry.get("data")
        if isinstance(details, dict):
            self._details_by_id[str(appid)] = details
            self._save_cache()
        return details

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
