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

RAWG_API_URL = "https://api.rawg.io/api/games"


class RAWGClient:
    def __init__(
        self,
        api_key: str,
        cache_path: str | Path,
        min_interval_s: float = 1.0,
    ):
        self.api_key = api_key
        self.cache_path = Path(cache_path)
        self.cache: Dict[str, Any] = load_json_cache(self.cache_path)
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)

    # ----------------------------
    # Main search
    # ----------------------------
    def search(self, game_name: str) -> Optional[Dict[str, Any]]:
        key = normalize_game_name(game_name)

        if key in self.cache:
            return self.cache[key]

        def _request():
            self.ratelimiter.wait()
            r = requests.get(
                RAWG_API_URL,
                params={
                    "search": game_name,
                    "page_size": 10,
                    "key": self.api_key,
                },
                timeout=10,
            )
            r.raise_for_status()
            return r.json()

        data = with_retries(_request, retries=3, on_fail_return=None)
        if not data or "results" not in data or not data["results"]:
            self.cache[key] = None
            save_json_cache(self.cache, self.cache_path)
            return None

        best, score = pick_best_match(game_name, data["results"], name_key="name")

        # Minimum threshold to accept the match
        if not best or score < 65:
            self.cache[key] = None
            save_json_cache(self.cache, self.cache_path)
            return None

        self.cache[key] = best
        save_json_cache(self.cache, self.cache_path)
        return best

    # ----------------------------
    # Metadata extraction
    # ----------------------------
    @staticmethod
    def extract_fields(rawg_obj: Dict[str, Any]) -> Dict[str, str]:
        if not rawg_obj:
            return {}

        genres = [g.get("name", "") for g in rawg_obj.get("genres", [])]
        platforms = [
            p.get("platform", {}).get("name", "")
            for p in rawg_obj.get("platforms", [])
        ]
        tags = [t.get("name", "") for t in rawg_obj.get("tags", [])]

        released = rawg_obj.get("released") or ""

        return {
            "RAWG_ID": str(rawg_obj.get("id", "")),
            "RAWG_Year": released[:4] if released else "",
            "RAWG_Genre": genres[0] if len(genres) > 0 else "",
            "RAWG_Genre2": genres[1] if len(genres) > 1 else "",
            "RAWG_Platforms": ", ".join(p for p in platforms if p),
            "RAWG_Tags": ", ".join(t for t in tags if t),
            "RAWG_Rating": str(rawg_obj.get("rating", "")),
            "RAWG_RatingsCount": str(rawg_obj.get("ratings_count", "")),
            "RAWG_Metacritic": str(rawg_obj.get("metacritic", "")),
        }
