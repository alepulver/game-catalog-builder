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

TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
IGDB_API_URL = "https://api.igdb.com/v4"


class IGDBClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        cache_path: str | Path,
        language: str = "en",
        min_interval_s: float = 0.3,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.language = (language or "en").strip() or "en"
        self.cache_path = Path(cache_path)
        self._by_id: Dict[str, Dict[str, str]] = {}
        self._by_name: Dict[str, Optional[str]] = {}
        self._load_cache(load_json_cache(self.cache_path))
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)

        self._token: Optional[str] = None
        self._ensure_token()

    # -------------------------------------------------
    # OAuth
    # -------------------------------------------------
    def _ensure_token(self):
        if self._token:
            return

        r = requests.post(
            TWITCH_TOKEN_URL,
            params={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
            timeout=10,
        )
        r.raise_for_status()
        self._token = r.json()["access_token"]

    def _headers(self):
        headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self._token}",
        }
        if self.language:
            headers["Accept-Language"] = self.language
        return headers

    # -------------------------------------------------
    # Helpers IGDB
    # -------------------------------------------------
    def _post(self, endpoint: str, query: str):
        def _request():
            self.ratelimiter.wait()
            r = requests.post(
                f"{IGDB_API_URL}/{endpoint}",
                headers=self._headers(),
                data=query,
                timeout=10,
            )
            r.raise_for_status()
            return r.json()

        return with_retries(_request, retries=3, on_fail_return=None)

    # -------------------------------------------------
    # Main search
    # -------------------------------------------------
    def search(self, game_name: str) -> Optional[Dict[str, Any]]:
        name_key = f"{self.language}:{normalize_game_name(game_name)}"
        if name_key in self._by_name:
            id_key = self._by_name[name_key]
            if not id_key:
                return None
            cached = self._by_id.get(str(id_key))
            if cached:
                return cached
            return None

        query = f'''
        search "{game_name}";
        fields id,name,
               genres.name,
               themes.name,
               game_modes.name,
               player_perspectives.name,
               franchises.name,
               game_engines.name,
               external_games.external_game_source,external_games.uid;
        limit 10;
        '''

        data = self._post("games", query)
        if not data or not isinstance(data, list) or len(data) == 0:
            # No results from API - log warning
            logging.warning(f"Not found in IGDB: '{game_name}'. No results from API.")
            self._by_name[name_key] = None
            self._save_cache()
            return None

        best, score, top_matches = pick_best_match(game_name, data, name_key="name")
        if not best or score < 65:
            # Log top 5 closest matches when not found
            if top_matches:
                top_names = [f"'{name}' ({s}%)" for name, s in top_matches[:5]]
                logging.warning(
                    f"Not found in IGDB: '{game_name}'. Closest matches: {', '.join(top_names)}"
                )
            else:
                logging.warning(f"Not found in IGDB: '{game_name}'. No matches found.")
            self._by_name[name_key] = None
            self._save_cache()
            return None

        # Warn if there are close matches (but not if it's a perfect 100% match)
        if top_matches and score < 100:
            top_names = [f"'{name}' ({s}%)" for name, s in top_matches[:5]]
            logging.warning(
                f"Close match for '{game_name}': Selected '{best.get('name', '')}' (score: {score}%), "
                f"alternatives: {', '.join(top_names)}"
            )

        enriched = self._extract_fields(best)
        igdb_id = enriched.get("IGDB_ID", "").strip()
        if igdb_id:
            id_key = f"{self.language}:{igdb_id}"
            self._by_id[id_key] = enriched
            self._by_name[name_key] = id_key
        else:
            self._by_name[name_key] = None
        self._save_cache()
        return enriched

    def _extract_fields(self, game: Dict[str, Any]) -> Dict[str, str]:
        def join_names(items: Any) -> str:
            if not items:
                return ""
            names: list[str] = []
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        name = str(item.get("name", "") or "").strip()
                        if name:
                            names.append(name)
                    elif isinstance(item, str):
                        s = item.strip()
                        if s:
                            names.append(s)
            return ", ".join(names)

        steam_appid = self._steam_appid_from_external_games(game.get("external_games", []) or [])

        return {
            "IGDB_ID": str(game.get("id", "") or ""),
            "IGDB_Genres": join_names(game.get("genres")),
            "IGDB_Themes": join_names(game.get("themes")),
            "IGDB_GameModes": join_names(game.get("game_modes")),
            "IGDB_Perspectives": join_names(game.get("player_perspectives")),
            "IGDB_Franchise": join_names(game.get("franchises")),
            "IGDB_Engine": join_names(game.get("game_engines")),
            "IGDB_SteamAppID": steam_appid,
        }

    def _steam_appid_from_external_games(self, external_games: list[Any]) -> str:
        """
        Extract Steam appid from IGDB external_games.

        Only uses data directly present in the game payload (no additional IGDB calls). This keeps
        the Steam mapping as a cheap cross-check rather than a dependency.
        """
        # Direct extraction if external_game_source is already present
        for ext in external_games:
            if not isinstance(ext, dict):
                continue
            source = ext.get("external_game_source")
            if source in (1, "1"):  # 1 == Steam (external_game_sources)
                return str(ext.get("uid") or "").strip()
        return ""

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return

        by_id = raw.get("by_id")
        by_name = raw.get("by_name")
        if not isinstance(by_id, dict) or not isinstance(by_name, dict):
            return

        self._by_id = {str(k): v for k, v in by_id.items() if isinstance(v, dict)}
        self._by_name = {str(k): (str(v) if v else None) for k, v in by_name.items()}

    def _save_cache(self) -> None:
        save_json_cache({"by_id": self._by_id, "by_name": self._by_name}, self.cache_path)
