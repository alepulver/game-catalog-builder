from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from ..config import IGDB, MATCHING, REQUEST, RETRY
from ..utils.utilities import (
    RateLimiter,
    extract_year_hint,
    iter_chunks,
    load_json_cache,
    pick_best_match,
    save_json_cache,
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
        min_interval_s: float = IGDB.min_interval_s,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.language = (language or "en").strip() or "en"
        self.cache_path = Path(cache_path)
        self.stats: dict[str, int] = {
            "by_query_hit": 0,
            "by_query_fetch": 0,
            "by_query_negative_hit": 0,
            "by_query_negative_fetch": 0,
            "by_id_hit": 0,
            "by_id_fetch": 0,
            "by_id_negative_hit": 0,
            "by_id_negative_fetch": 0,
        }
        # Cache raw IGDB game payloads keyed by id (language:id). Derived output fields are
        # computed on-demand to keep caches independent of code changes.
        self._by_id: dict[str, Any] = {}
        # Cache by exact query string, storing only lightweight candidates
        # (id/name/first_release_date).
        self._by_query: dict[str, list[dict[str, Any]]] = {}
        self._load_cache(load_json_cache(self.cache_path))
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)

        self._token: str | None = None
        # Token is acquired lazily on first API request. This allows cached-only re-runs to work
        # even without network access and avoids unnecessary OAuth calls.

    # -------------------------------------------------
    # OAuth
    # -------------------------------------------------
    def _ensure_token(self):
        if self._token:
            return

        # Use form-encoded body (not URL params) to avoid leaking secrets in tracebacks/logs.
        r = requests.post(
            TWITCH_TOKEN_URL,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
            timeout=REQUEST.timeout_s,
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
            self._ensure_token()
            self.ratelimiter.wait()
            r = requests.post(
                f"{IGDB_API_URL}/{endpoint}",
                headers=self._headers(),
                data=query,
                timeout=REQUEST.timeout_s,
            )
            r.raise_for_status()
            return r.json()

        return with_retries(
            _request,
            retries=RETRY.retries,
            on_fail_return=None,
            context=f"IGDB POST /{endpoint}",
        )

    def _post_cached(self, endpoint: str, query: str) -> Any:
        qkey = f"{self.language}:{endpoint}:{query.strip()}"
        cached = self._by_query.get(qkey)
        if cached is not None:
            self.stats["by_query_hit"] += 1
            if not cached:
                self.stats["by_query_negative_hit"] += 1
            return cached

        data = self._post(endpoint, query)
        if not isinstance(data, list):
            return data

        # Populate by_id with raw game payloads and store only lightweight candidates under
        # by_query.
        candidates: list[dict[str, Any]] = []
        for it in data:
            if not isinstance(it, dict):
                continue
            gid = it.get("id")
            if gid is None:
                continue
            id_key = f"{self.language}:{gid}"
            self._by_id[id_key] = it
            candidates.append(
                {
                    "id": gid,
                    "name": it.get("name", ""),
                    "first_release_date": it.get("first_release_date"),
                }
            )
        self._by_query[qkey] = candidates
        self._save_cache()
        self.stats["by_query_fetch"] += 1
        if not candidates:
            self.stats["by_query_negative_fetch"] += 1
        return candidates

    def get_by_id(self, igdb_id: int | str) -> dict[str, Any] | None:
        """
        Fetch an IGDB game by id (preferring cache).
        """
        igdb_id_str = str(igdb_id).strip()
        if not igdb_id_str:
            return None

        return self.get_by_ids([igdb_id_str]).get(igdb_id_str)

    def get_by_ids(self, igdb_ids: list[int | str]) -> dict[str, dict[str, Any]]:
        """
        Fetch multiple IGDB games by id in as few API calls as possible.

        Returns a mapping of IGDB id (string) -> extracted IGDB_* fields dict.
        """
        ids: list[str] = []
        for x in igdb_ids:
            s = str(x).strip()
            if s and s.isdigit():
                ids.append(s)
        if not ids:
            return {}

        out: dict[str, dict[str, Any]] = {}
        missing: list[str] = []

        for igdb_id_str in ids:
            id_key = f"{self.language}:{igdb_id_str}"
            cached = self._by_id.get(id_key)
            if isinstance(cached, dict):
                self.stats["by_id_hit"] += 1
                out[igdb_id_str] = self._extract_fields(cached)
            elif cached is None and id_key in self._by_id:
                self.stats["by_id_negative_hit"] += 1
            else:
                missing.append(igdb_id_str)

        base_fields = """
        fields id,name,first_release_date,
               summary,storyline,
               rating,rating_count,
               category,status,
               websites.url,
               platforms.name,
               genres.name,
               themes.name,
               keywords.name,
               game_modes.name,
               player_perspectives.name,
               franchises.name,
               game_engines.name,
               collections.name,
               parent_game.name,
               version_parent.name,
               dlcs.name,
               expansions.name,
               ports.name,
               involved_companies.company.name,
               involved_companies.developer,
               involved_companies.publisher,
               age_ratings.category,
               age_ratings.rating,
               external_games.external_game_source,external_games.uid;
        """

        for chunk in iter_chunks(missing, IGDB.get_by_ids_batch_size):
            ids_expr = ",".join(chunk)
            query = f"""
            {base_fields}
            where id = ({ids_expr});
            limit {len(chunk)};
            """
            data = self._post("games", query)
            if data is None:
                continue
            if not isinstance(data, list):
                continue
            fetched_any = False
            fetched_ids: set[str] = set()
            for it in data:
                if not isinstance(it, dict):
                    continue
                gid = it.get("id")
                if gid is None:
                    continue
                gid_str = str(gid).strip()
                if not gid_str:
                    continue
                id_key = f"{self.language}:{gid_str}"
                self._by_id[id_key] = it
                fetched_any = True
                self.stats["by_id_fetch"] += 1
                fetched_ids.add(gid_str)
                out[gid_str] = self._extract_fields(it)
            negative_any = False
            for want in chunk:
                if want not in fetched_ids:
                    self._by_id[f"{self.language}:{want}"] = None
                    self.stats["by_id_negative_fetch"] += 1
                    negative_any = True
            if fetched_any or negative_any:
                self._save_cache()

        return out

    # -------------------------------------------------
    # Main search
    # -------------------------------------------------
    def search(self, game_name: str, year_hint: int | None = None) -> dict[str, Any] | None:
        def _strip_trailing_paren_year(s: str) -> str:
            y = extract_year_hint(s)
            if y is None:
                return s
            return re.sub(r"\s*\(\s*(19\d{2}|20\d{2})\s*\)\s*$", "", s).strip() or s

        stripped_name = _strip_trailing_paren_year(str(game_name or "").strip())

        # IGDB search is often more reliable without a trailing year token; prefer a stripped
        # query, but keep the original name for cache keys and logging.
        search_text = stripped_name or str(game_name or "").strip()
        # IGDB uses a query DSL with `search "..."`; escape quotes/backslashes and strip control
        # characters to avoid 400s for odd titles.
        search_text = search_text.replace("\\", "\\\\").replace('"', '\\"')
        search_text = re.sub(r"[\r\n\t]+", " ", search_text).strip()
        base_fields = """
        fields id,name,first_release_date,
               summary,storyline,
               rating,rating_count,
               category,status,
               websites.url,
               platforms.name,
               genres.name,
               themes.name,
               keywords.name,
               game_modes.name,
               player_perspectives.name,
               franchises.name,
               game_engines.name,
               collections.name,
               parent_game.name,
               version_parent.name,
               dlcs.name,
               expansions.name,
               ports.name,
               involved_companies.company.name,
               involved_companies.developer,
               involved_companies.publisher,
               age_ratings.category,
               age_ratings.rating,
               external_games.external_game_source,external_games.uid;
        """

        data = None
        if year_hint is not None:
            # Try a narrow year window first to avoid common pitfalls like sequels, remakes,
            # and upcoming placeholders (e.g. "Silent Hill f").
            start = int(datetime(int(year_hint) - 1, 1, 1, tzinfo=timezone.utc).timestamp())
            end = int(
                datetime(int(year_hint) + 1, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp()
            )
            query = f'''
            search "{search_text}";
            {base_fields}
            where first_release_date != null
              & first_release_date >= {start}
              & first_release_date <= {end};
            limit {IGDB.search_limit};
            '''
            data = self._post_cached("games", query)

        if not data:
            query = f'''
            search "{search_text}";
            {base_fields}
            limit {IGDB.search_limit};
            '''
            data = self._post_cached("games", query)
        if data is None:
            logging.warning(
                f"IGDB search request failed for '{game_name}' (no response); "
                "not caching as not-found."
            )
            return None
        if not data or not isinstance(data, list) or len(data) == 0:
            # No results from API - log warning
            logging.warning(f"Not found in IGDB: '{game_name}'. No results from API.")
            return None

        def _year_getter(obj: dict[str, Any]) -> int | None:
            ts = obj.get("first_release_date")
            if isinstance(ts, (int, float)) and ts > 0:
                try:
                    return int(datetime.fromtimestamp(ts, tz=timezone.utc).year)
                except Exception:
                    return None
            return None

        best, score, top_matches = pick_best_match(
            search_text,
            data,
            name_key="name",
            year_hint=year_hint,
            year_getter=_year_getter,
        )
        if not best or score < MATCHING.min_score:
            # Log top 5 closest matches when not found
            if top_matches:
                top_names = [f"'{name}' ({s}%)" for name, s in top_matches[:5]]
                logging.warning(
                    f"Not found in IGDB: '{game_name}'. Closest matches: {', '.join(top_names)}"
                )
            else:
                logging.warning(f"Not found in IGDB: '{game_name}'. No matches found.")
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

        igdb_id = str(best.get("id") or "").strip()
        if igdb_id:
            raw = self._by_id.get(f"{self.language}:{igdb_id}")
            if not isinstance(raw, dict):
                # Partial migration case: fetch raw by id.
                return self.get_by_id(igdb_id)
            return self._extract_fields(raw)
        return None

    def format_cache_stats(self) -> str:
        s = self.stats
        return (
            f"by_query hit={s['by_query_hit']} fetch={s['by_query_fetch']} "
            f"(neg hit={s['by_query_negative_hit']} fetch={s['by_query_negative_fetch']}), "
            f"by_id hit={s['by_id_hit']} fetch={s['by_id_fetch']} "
            f"(neg hit={s['by_id_negative_hit']} fetch={s['by_id_negative_fetch']})"
        )

    def _extract_fields(self, game: dict[str, Any]) -> dict[str, str]:
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
        year = ""
        ts = game.get("first_release_date")
        if isinstance(ts, (int, float)) and ts > 0:
            try:
                year = str(datetime.fromtimestamp(ts, tz=timezone.utc).year)
            except Exception:
                year = ""

        rating = game.get("rating", None)
        rating_count = game.get("rating_count", None)
        score_100 = ""
        if isinstance(rating, (int, float)):
            try:
                score_100 = str(int(round(float(rating))))
            except Exception:
                score_100 = ""

        return {
            "IGDB_ID": str(game.get("id", "") or ""),
            "IGDB_Name": str(game.get("name", "") or ""),
            "IGDB_Year": year,
            "IGDB_Platforms": join_names(game.get("platforms")),
            "IGDB_Genres": join_names(game.get("genres")),
            "IGDB_Themes": join_names(game.get("themes")),
            "IGDB_GameModes": join_names(game.get("game_modes")),
            "IGDB_Perspectives": join_names(game.get("player_perspectives")),
            "IGDB_Franchise": join_names(game.get("franchises")),
            "IGDB_Engine": join_names(game.get("game_engines")),
            "IGDB_SteamAppID": steam_appid,
            "IGDB_Rating": str(rating if rating is not None else ""),
            "IGDB_RatingCount": str(rating_count if rating_count is not None else ""),
            "Score_IGDB_100": score_100,
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
            # Some cached payloads (or older exports) can use a friendlier shape:
            #   { category: "steam", uid: "620" }
            category = str(ext.get("category") or "").strip().lower()
            if category == "steam":
                return str(ext.get("uid") or "").strip()
        return ""

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return

        by_id = raw.get("by_id")
        by_query = raw.get("by_query")
        if isinstance(by_id, dict):
            # Only keep raw IGDB game dicts here. Legacy extracted dicts (IGDB_*) are ignored;
            # they should be migrated in-place from by_query where raw payloads are available.
            self._by_id = {
                str(k): v
                for k, v in by_id.items()
                if (isinstance(v, dict) and "id" in v) or v is None
            }
        if isinstance(by_query, dict):
            out: dict[str, list[dict[str, Any]]] = {}
            for k, v in by_query.items():
                if isinstance(v, list):
                    out[str(k)] = [it for it in v if isinstance(it, dict)]
            self._by_query = out

    def _save_cache(self) -> None:
        save_json_cache(
            {
                "by_id": self._by_id,
                "by_query": self._by_query,
            },
            self.cache_path,
        )
