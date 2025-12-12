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

TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
IGDB_API_URL = "https://api.igdb.com/v4"


class IGDBClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        cache_path: str | Path,
        min_interval_s: float = 0.8,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.cache_path = Path(cache_path)
        self.cache: Dict[str, Any] = load_json_cache(self.cache_path)
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
        return {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self._token}",
        }

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
        key = normalize_game_name(game_name)
        if key in self.cache:
            return self.cache[key]

        query = f'''
        search "{game_name}";
        fields id,name,genres,themes,game_modes,player_perspectives,
               franchises,game_engines,involved_companies;
        limit 10;
        '''

        data = self._post("games", query)
        if not data:
            self.cache[key] = None
            save_json_cache(self.cache, self.cache_path)
            return None

        best, score = pick_best_match(game_name, data, name_key="name")
        if not best or score < 65:
            self.cache[key] = None
            save_json_cache(self.cache, self.cache_path)
            return None

        # Resolve IDs to names
        enriched = self._resolve_fields(best)
        self.cache[key] = enriched
        save_json_cache(self.cache, self.cache_path)
        return enriched

    # -------------------------------------------------
    # Resolve IDs → names
    # -------------------------------------------------
    def _resolve_ids(self, endpoint: str, ids: list[int], field="name") -> list[str]:
        if not ids:
            return []

        ids_str = ",".join(str(i) for i in ids)
        query = f"fields {field}; where id=({ids_str}); limit 50;"
        data = self._post(endpoint, query)
        if not data:
            return []
        return [d.get(field, "") for d in data if field in d]

    def _resolve_fields(self, game: Dict[str, Any]) -> Dict[str, str]:
        genres = self._resolve_ids("genres", game.get("genres", []))
        themes = self._resolve_ids("themes", game.get("themes", []))
        modes = self._resolve_ids("game_modes", game.get("game_modes", []))
        perspectives = self._resolve_ids("player_perspectives", game.get("player_perspectives", []))
        engines = self._resolve_ids("game_engines", game.get("game_engines", []))
        franchises = self._resolve_ids("franchises", game.get("franchises", []))

        return {
            "IGDB_ID": str(game.get("id", "")),
            "IGDB_Genres": ", ".join(genres),
            "IGDB_Themes": ", ".join(themes),
            "IGDB_GameModes": ", ".join(modes),
            "IGDB_Perspectives": ", ".join(perspectives),
            "IGDB_Franchise": ", ".join(franchises),
            "IGDB_Engine": ", ".join(engines),
        }
