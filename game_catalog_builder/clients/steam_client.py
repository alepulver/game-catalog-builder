from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import requests

from ..utils.utilities import (
    RateLimiter,
    load_json_cache,
    normalize_game_name,
    pick_best_match,
    save_json_cache,
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
        self._by_id: dict[str, Any] = {}
        self._by_name: dict[str, str | None] = {}
        self._details_by_id: dict[str, Any] = {}
        self._load_cache(load_json_cache(self.cache_path))
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return

        by_id = raw.get("by_id")
        by_name = raw.get("by_name")
        by_details = raw.get("by_details")
        if not (isinstance(by_id, dict) and isinstance(by_name, dict)):
            logging.warning(
                "Steam cache file is in an incompatible format; ignoring it (delete it to rebuild)."
            )
            return
        self._by_id = {str(k): v for k, v in by_id.items()}
        self._by_name = {str(k): (str(v) if v else None) for k, v in by_name.items()}
        if isinstance(by_details, dict):
            self._details_by_id = {str(k): v for k, v in by_details.items()}

    def _save_cache(self) -> None:
        save_json_cache(
            {"by_id": self._by_id, "by_name": self._by_name, "by_details": self._details_by_id},
            self.cache_path,
        )

    # -------------------------------------------------
    # Search AppID by name
    # -------------------------------------------------
    def search_appid(self, game_name: str) -> dict[str, Any] | None:
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
        if score < 100:
            msg = (
                f"Close match for '{game_name}': Selected '{best.get('name', '')}' "
                f"(score: {score}%)"
            )
            if top_matches:
                top_names = [f"'{name}' ({s}%)" for name, s in top_matches[:5]]
                msg += f", alternatives: {', '.join(top_names)}"
            logging.warning(msg)

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
    def get_app_details(self, appid: int) -> dict[str, Any] | None:
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
    def extract_fields(appid: int, details: dict[str, Any]) -> dict[str, str]:
        if not details:
            return {}

        def extract_year(text: str) -> str:
            m = re.search(r"\b(19\d{2}|20\d{2})\b", text or "")
            return m.group(1) if m else ""

        price = ""
        if details.get("is_free"):
            price = "Free"
        elif "price_overview" in details:
            price = str(details["price_overview"].get("final_formatted", ""))

        categories = [c.get("description", "") for c in details.get("categories", [])]

        # Real Steam tags come indirectly from genres + categories + metadata
        genres = [g.get("description", "") for g in details.get("genres", [])]

        recommendations = details.get("recommendations", {})
        review_count = recommendations.get("total", "")

        release = details.get("release_date", {}) or {}
        release_year = extract_year(str(release.get("date", "") or ""))

        platforms = details.get("platforms", {}) or {}
        platform_names: list[str] = []
        if platforms.get("windows"):
            platform_names.append("Windows")
        if platforms.get("mac"):
            platform_names.append("macOS")
        if platforms.get("linux"):
            platform_names.append("Linux")

        return {
            "Steam_AppID": str(appid),
            "Steam_Name": str(details.get("name", "") or ""),
            "Steam_ReleaseYear": release_year,
            "Steam_Platforms": ", ".join(platform_names),
            "Steam_Tags": ", ".join(genres),
            "Steam_ReviewCount": str(review_count),
            "Steam_ReviewPercent": "",  # Steam doesn't expose this directly without extra scraping
            "Steam_Price": price,
            "Steam_Categories": ", ".join(categories),
        }
