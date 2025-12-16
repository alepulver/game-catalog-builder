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

RAWG_API_URL = "https://api.rawg.io/api/games"


class RAWGClient:
    def __init__(
        self,
        api_key: str,
        cache_path: str | Path,
        language: str = "en",
        min_interval_s: float = 1.0,
    ):
        self.api_key = api_key
        self.language = (language or "en").strip() or "en"
        self.cache_path = Path(cache_path)
        self._by_id: dict[str, Any] = {}
        self._by_name: dict[str, str | None] = {}
        self._load_cache(load_json_cache(self.cache_path))
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)

    def _id_key(self, rawg_id: int | str) -> str:
        return f"{self.language}:{rawg_id}"

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return

        by_id = raw.get("by_id")
        by_name = raw.get("by_name")
        if not (isinstance(by_id, dict) and isinstance(by_name, dict)):
            logging.warning(
                "RAWG cache file is in an incompatible format; ignoring it (delete it to rebuild)."
            )
            return
        self._by_id = {str(k): v for k, v in by_id.items()}
        self._by_name = {str(k): (str(v) if v else None) for k, v in by_name.items()}

    def _save_cache(self) -> None:
        save_json_cache({"by_id": self._by_id, "by_name": self._by_name}, self.cache_path)

    def get_by_id(self, rawg_id: int | str) -> dict[str, Any] | None:
        """
        Fetch a RAWG game by id (preferring cache).
        """
        rawg_id_str = str(rawg_id).strip()
        if not rawg_id_str:
            return None

        id_key = self._id_key(rawg_id_str)
        cached = self._by_id.get(id_key)
        if isinstance(cached, dict):
            return cached

        def _request():
            self.ratelimiter.wait()
            r = requests.get(
                f"{RAWG_API_URL}/{rawg_id_str}",
                params={"key": self.api_key, "lang": self.language},
                timeout=10,
            )
            r.raise_for_status()
            return r.json()

        data = with_retries(_request, retries=3, on_fail_return=None)
        if isinstance(data, dict) and data.get("id") is not None:
            self._by_id[id_key] = data
            self._save_cache()
            return data
        return None

    # ----------------------------
    # Main search
    # ----------------------------
    def search(self, game_name: str) -> dict[str, Any] | None:
        name_key = f"{self.language}:{normalize_game_name(game_name)}"

        if name_key in self._by_name:
            id_key = self._by_name[name_key]
            if not id_key:
                return None
            return self._by_id.get(id_key)

        def _request():
            self.ratelimiter.wait()
            r = requests.get(
                RAWG_API_URL,
                params={
                    "search": game_name,
                    "page_size": 10,
                    "key": self.api_key,
                    "lang": self.language,
                },
                timeout=10,
            )
            r.raise_for_status()
            return r.json()

        data = with_retries(_request, retries=3, on_fail_return=None)
        if not data or "results" not in data or not data["results"]:
            # No results from API - log warning
            logging.warning(f"Not found in RAWG: '{game_name}'. No results from API.")
            self._by_name[name_key] = None
            self._save_cache()
            return None

        best, score, top_matches = pick_best_match(game_name, data["results"], name_key="name")

        # Minimum threshold to accept the match
        if not best or score < 65:
            # Log top 5 closest matches when not found
            if top_matches:
                top_names = [f"'{name}' ({s}%)" for name, s in top_matches[:5]]
                logging.warning(
                    f"Not found in RAWG: '{game_name}'. Closest matches: {', '.join(top_names)}"
                )
            else:
                logging.warning(f"Not found in RAWG: '{game_name}'. No matches found.")
            self._by_name[name_key] = None
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

        rawg_id = best.get("id")
        if rawg_id is not None:
            id_key = self._id_key(rawg_id)
            self._by_id[id_key] = best
            self._by_name[name_key] = id_key
            self._save_cache()
        return best

    # ----------------------------
    # Metadata extraction
    # ----------------------------
    @staticmethod
    def extract_fields(rawg_obj: dict[str, Any]) -> dict[str, str]:
        if not rawg_obj:
            return {}

        genres = [g.get("name", "") for g in rawg_obj.get("genres", [])]
        platforms = [p.get("platform", {}).get("name", "") for p in rawg_obj.get("platforms", [])]
        tags = [t.get("name", "") for t in rawg_obj.get("tags", [])]
        # RAWG tags can contain mixed-language duplicates; drop Cyrillic tags by default.
        tags = [t for t in tags if t and not re.search(r"[А-Яа-яЁё]", t)]

        released = rawg_obj.get("released") or ""

        return {
            "RAWG_ID": str(rawg_obj.get("id", "")),
            "RAWG_Name": str(rawg_obj.get("name", "") or ""),
            "RAWG_Released": str(released or ""),
            "RAWG_Year": released[:4] if released else "",
            "RAWG_Genre": genres[0] if len(genres) > 0 else "",
            "RAWG_Genre2": genres[1] if len(genres) > 1 else "",
            "RAWG_Platforms": ", ".join(p for p in platforms if p),
            "RAWG_Tags": ", ".join(t for t in tags if t),
            "RAWG_Rating": str(rawg_obj.get("rating", "")),
            "RAWG_RatingsCount": str(rawg_obj.get("ratings_count", "")),
            "RAWG_Metacritic": str(rawg_obj.get("metacritic", "")),
        }
