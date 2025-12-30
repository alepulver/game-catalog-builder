from __future__ import annotations

import logging
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from ..config import IGDB, MATCHING, REQUEST, RETRY
from ..utils.utilities import (
    CacheIOTracker,
    RateLimiter,
    extract_year_hint,
    iter_chunks,
    normalize_game_name,
    pick_best_match,
)
from .parse import as_int, as_str, get_list_of_dicts, normalize_str_list, year_from_epoch_seconds
from .http_client import ConfiguredHTTPJSONClient, HTTPJSONClient, HTTPRequestDefaults

TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
IGDB_API_URL = "https://api.igdb.com/v4"
_IGDB_BAD_REQUEST = object()


class IGDBClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        cache_path: str | Path,
        language: str = "en",
        min_interval_s: float = IGDB.min_interval_s,
    ):
        self._session = requests.Session()
        base_http = HTTPJSONClient(self._session, stats=None)
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
            # HTTP request counters (attempts, including retries).
            "http_oauth_token": 0,
            "http_post": 0,
        }
        base_http.stats = self.stats
        # Cache raw IGDB game payloads keyed by id (language:id). Derived output fields are
        # computed on-demand to keep caches independent of code changes.
        self._by_id: dict[str, Any] = {}
        # Cache by exact query string, storing the raw IGDB response payload (list of game dicts).
        self._by_query: dict[str, list[dict[str, Any]]] = {}
        self._cache_io = CacheIOTracker(self.stats)
        self._load_cache(self._cache_io.load_json(self.cache_path))
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)
        self._post_http = ConfiguredHTTPJSONClient(
            base_http,
            HTTPRequestDefaults(
                ratelimiter=self.ratelimiter,
                retries=RETRY.retries,
                counter_key="http_post",
                context_prefix="IGDB POST",
            ),
        )

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
        self.stats["http_oauth_token"] += 1
        r = self._session.post(
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
        self._ensure_token()
        resp = self._post_http.post_json(
            f"{IGDB_API_URL}/{endpoint}",
            headers=self._headers(),
            data=query,
            status_handlers={400: _IGDB_BAD_REQUEST},
            context=f"/{endpoint}",
            on_fail_return=None,
        )
        if resp is _IGDB_BAD_REQUEST:
            logging.error(f"[HTTP] IGDB POST: /{endpoint}: 400 Bad Request (query rejected)")
        return resp

    def _post_cached(self, endpoint: str, query: str) -> Any:
        qkey = f"{self.language}:{endpoint}:{query.strip()}"
        cached = self._by_query.get(qkey)
        if cached is not None:
            self.stats["by_query_hit"] += 1
            if not cached:
                self.stats["by_query_negative_hit"] += 1
            return cached

        data = self._post(endpoint, query)
        if data is None:
            return None
        if not isinstance(data, list):
            return data

        # Cache the exact raw response under by_query, and populate by_id from it.
        raw_items: list[dict[str, Any]] = []
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
            raw_items.append(it)
        self._by_query[qkey] = raw_items
        self._save_cache()
        self.stats["by_query_fetch"] += 1
        if not raw_items:
            self.stats["by_query_negative_fetch"] += 1
        return raw_items

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
                out[igdb_id_str] = self._extract_metrics(cached)
            elif cached is None and id_key in self._by_id:
                self.stats["by_id_negative_hit"] += 1
            else:
                missing.append(igdb_id_str)

        base_fields = """
        fields id,name,first_release_date,
               summary,storyline,
               rating,rating_count,
               aggregated_rating,aggregated_rating_count,
               category,status,
               alternative_names.name,
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
                out[gid_str] = self._extract_metrics(it)
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
    @staticmethod
    def _select_best_match(
        *,
        query: str,
        results: list[dict[str, Any]],
        year_hint: int | None,
    ) -> tuple[dict[str, Any] | None, int, list[tuple[str, int]]]:
        def _year_getter(obj: dict[str, Any]) -> int | None:
            ts = obj.get("first_release_date")
            if isinstance(ts, (int, float)) and ts > 0:
                try:
                    return int(datetime.fromtimestamp(ts, tz=timezone.utc).year)
                except Exception:
                    return None
            return None

        def _norm(s: str) -> str:
            return re.sub(r"\s{2,}", " ", normalize_game_name(str(s or ""))).strip()

        def _series_numbers(name: str) -> set[int]:
            toks = normalize_game_name(name).split()
            out: set[int] = set()
            for t in toks:
                if not t.isdecimal():
                    continue
                if len(t) > 1 and t.startswith("0"):
                    continue
                try:
                    n = int(t)
                except ValueError:
                    continue
                if n == 0:
                    continue
                if 1900 <= n <= 2100:
                    continue
                if 0 < n <= 50:
                    out.add(n)
            return out

        def _looks_dlc_like(name: str) -> bool:
            toks = set(normalize_game_name(name).split())
            dlc_like = {
                "soundtrack",
                "demo",
                "beta",
                "dlc",
                "expansion",
                "pack",
                "season",
                "pass",
            }
            return any(t in toks for t in dlc_like)

        query_dlc_like = _looks_dlc_like(query)
        q_norm = _norm(query)

        # Prefer exact normalized title matches when present.
        exact = [it for it in results if _norm(str(it.get("name", "") or "")) == q_norm]
        if exact and len(exact) < len(results):
            results = exact

        # Prefer "main game" entries when present, unless the user clearly asked for DLC-like
        # content. (IGDB uses category=0 for main game; other categories include DLC/expansion/etc.)
        if not query_dlc_like:
            main = [it for it in results if it.get("category") in (0, "0", None)]
            # Only narrow when it actually reduces candidates and keeps at least one result.
            if main and len(main) < len(results):
                results = main

        # If the query has no sequel number, prefer candidates without explicit sequel numbers
        # when alternatives exist.
        if q_norm and not _series_numbers(q_norm):
            no_nums = [it for it in results if not _series_numbers(str(it.get("name", "") or ""))]
            if no_nums and len(no_nums) < len(results):
                results = no_nums

        # Avoid DLC/demo/soundtrack-like matches unless explicitly requested.
        if not query_dlc_like:
            non_dlc = [it for it in results if not _looks_dlc_like(str(it.get("name", "") or ""))]
            if non_dlc and len(non_dlc) < len(results):
                results = non_dlc

        if year_hint is not None:
            tol = int(MATCHING.year_hint_tolerance)
            near = []
            for it in results:
                y = _year_getter(it)
                if y is None:
                    continue
                if abs(int(y) - int(year_hint)) <= tol:
                    near.append(it)
            if near and len(near) < len(results):
                results = near

        return pick_best_match(
            query,
            results,
            name_key="name",
            year_hint=year_hint,
            year_getter=_year_getter,
        )

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
        # Normalize to NFKC to avoid odd Unicode digits (e.g. "²" -> "2") and compatibility chars.
        search_text = unicodedata.normalize("NFKC", search_text)
        # Remove control characters (including uncommon unicode separators) to avoid query errors.
        search_text = "".join((" " if unicodedata.category(ch).startswith("C") else ch) for ch in search_text)
        # IGDB uses a query DSL with `search "..."`; escape quotes/backslashes and strip control
        # characters to avoid 400s for odd titles.
        search_text = search_text.replace("\\", "\\\\").replace('"', '\\"')
        search_text = re.sub(r"[\r\n\t]+", " ", search_text).strip()
        base_fields = """
        fields id,name,first_release_date,
               summary,storyline,
               rating,rating_count,
               aggregated_rating,aggregated_rating_count,
               category,status,
               alternative_names.name,
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
            end = int(datetime(int(year_hint) + 1, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp())
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
        if data is _IGDB_BAD_REQUEST:
            # Fallback: try a simpler normalized term to avoid DSL parsing issues.
            fallback = normalize_game_name(stripped_name or str(game_name or "")).strip()
            fallback = unicodedata.normalize("NFKC", fallback)
            fallback = "".join((" " if unicodedata.category(ch).startswith("C") else ch) for ch in fallback)
            fallback = fallback.replace("\\", "\\\\").replace('"', '\\"')
            fallback = re.sub(r"[\r\n\t]+", " ", fallback).strip()
            if fallback and fallback != search_text:
                query = f'''
                search "{fallback}";
                {base_fields}
                limit {IGDB.search_limit};
                '''
                data = self._post_cached("games", query)
        if data is None:
            logging.warning(
                f"IGDB search request failed for '{game_name}' (no response); not caching as not-found."
            )
            return None
        if not data or not isinstance(data, list) or len(data) == 0:
            # No results from API - log warning
            logging.warning(f"Not found in IGDB: '{game_name}'. No results from API.")
            return None

        best, score, top_matches = self._select_best_match(
            query=search_text,
            results=data,
            year_hint=year_hint,
        )
        if not best or score < MATCHING.min_score:
            # Log top 5 closest matches when not found
            if top_matches:
                top_names = [f"'{name}' ({s}%)" for name, s in top_matches[:5]]
                logging.warning(f"Not found in IGDB: '{game_name}'. Closest matches: {', '.join(top_names)}")
            else:
                logging.warning(f"Not found in IGDB: '{game_name}'. No matches found.")
            return None

        # Warn if there are close matches (but not if it's a perfect 100% match)
        if score < 100:
            msg = f"Close match for '{game_name}': Selected '{best.get('name', '')}' (score: {score}%)"
            if top_matches:
                top_names = [f"'{name}' ({s}%)" for name, s in top_matches[:5]]
                msg += f", alternatives: {', '.join(top_names)}"
            logging.warning(msg)

        igdb_id = str(best.get("id") or "").strip()
        if igdb_id:
            raw = self._by_id.get(f"{self.language}:{igdb_id}")
            if isinstance(raw, dict):
                return self._extract_metrics(raw)
            logging.warning(
                f"IGDB cache missing by_id payload for '{game_name}': id={igdb_id}. Delete cache to rebuild."
            )
            return None
        return None

    def format_cache_stats(self) -> str:
        s = self.stats
        base = (
            f"by_query hit={s['by_query_hit']} fetch={s['by_query_fetch']} "
            f"(neg hit={s['by_query_negative_hit']} fetch={s['by_query_negative_fetch']}), "
            f"by_id hit={s['by_id_hit']} fetch={s['by_id_fetch']} "
            f"(neg hit={s['by_id_negative_hit']} fetch={s['by_id_negative_fetch']}), "
            f"http oauth={s['http_oauth_token']} {HTTPJSONClient.format_timing(s, key='http_post')}"
        )
        base += f", {CacheIOTracker.format_io(s)}"
        http_429 = int(s.get("http_429", 0) or 0)
        if http_429:
            return (
                base
                + f", 429={http_429} retries={int(s.get('http_429_retries', 0) or 0)}"
                + f" backoff_ms={int(s.get('http_429_backoff_ms', 0) or 0)}"
            )
        return base

    def get_alternative_names(self, igdb_id: str | int) -> list[str]:
        """
        Return IGDB alternative_names.name values from cached raw payload (no network).
        """
        igdb_id_str = str(igdb_id or "").strip()
        if not igdb_id_str:
            return []
        raw = self._by_id.get(f"{self.language}:{igdb_id_str}")
        if not isinstance(raw, dict):
            return []
        items = raw.get("alternative_names") or []
        if not isinstance(items, list):
            return []
        names: list[str] = []
        for it in items:
            if not isinstance(it, dict):
                continue
            n = str(it.get("name") or "").strip()
            if n:
                names.append(n)
        # Dedup while preserving order.
        seen: set[str] = set()
        return [n for n in names if not (n in seen or seen.add(n))]

    def _extract_metrics(self, game: dict[str, Any]) -> dict[str, object]:
        def names_list(items: Any) -> list[str]:
            if not items:
                return []
            if isinstance(items, dict):
                return normalize_str_list([as_str(items.get("name"))])
            if isinstance(items, list):
                out: list[str] = []
                for item in items:
                    if isinstance(item, dict):
                        out.append(as_str(item.get("name")))
                    elif isinstance(item, str):
                        out.append(as_str(item))
                return normalize_str_list(out)
            return []

        def _truncate(text: object, max_len: int = 500) -> str:
            s = as_str(text)
            if not s:
                return ""
            if len(s) <= max_len:
                return s
            return s[:max_len].rstrip() + "…"

        steam_appid = self._steam_appid_from_external_games(game.get("external_games", []) or [])
        involved = game.get("involved_companies", []) or []
        dev_list: list[str] = []
        pub_list: list[str] = []
        if isinstance(involved, list):
            for ic in involved:
                if not isinstance(ic, dict):
                    continue
                company = ic.get("company") or {}
                cname = as_str((company.get("name") if isinstance(company, dict) else ""))
                if not cname:
                    continue
                if ic.get("developer") is True:
                    dev_list.append(cname)
                if ic.get("publisher") is True:
                    pub_list.append(cname)
        dev_list = normalize_str_list(dev_list)
        pub_list = normalize_str_list(pub_list)

        year = year_from_epoch_seconds(game.get("first_release_date"))

        rating = game.get("rating", None)
        score_100: int | None = None
        if isinstance(rating, (int, float)):
            score_100 = int(round(float(rating)))

        agg = game.get("aggregated_rating", None)
        agg_100: int | None = None
        if isinstance(agg, (int, float)):
            agg_100 = int(round(float(agg)))

        rating_count = as_int(game.get("rating_count"))
        agg_count = as_int(game.get("aggregated_rating_count"))

        websites: list[str] = []
        for w in get_list_of_dicts(game.get("websites")):
            url = as_str(w.get("url"))
            if url and url not in websites:
                websites.append(url)
        websites = websites[:5]

        return {
            "igdb.id": as_str(game.get("id")),
            "igdb.name": as_str(game.get("name")),
            "igdb.year": year,
            "igdb.summary": _truncate(game.get("summary", "")),
            "igdb.websites": websites,
            "igdb.alternative_names": names_list(game.get("alternative_names")),
            "igdb.relationships.parent_game": (names_list(game.get("parent_game")) or [""])[0].strip(),
            "igdb.relationships.version_parent": (names_list(game.get("version_parent")) or [""])[0].strip(),
            "igdb.relationships.dlcs": names_list(game.get("dlcs")),
            "igdb.relationships.expansions": names_list(game.get("expansions")),
            "igdb.relationships.ports": names_list(game.get("ports")),
            "igdb.platforms": names_list(game.get("platforms")),
            "igdb.genres": names_list(game.get("genres")),
            "igdb.themes": names_list(game.get("themes")),
            "igdb.keywords": names_list(game.get("keywords")),
            "igdb.game_modes": names_list(game.get("game_modes")),
            "igdb.perspectives": names_list(game.get("player_perspectives")),
            "igdb.franchise": names_list(game.get("franchises")),
            "igdb.engine": names_list(game.get("game_engines")),
            "igdb.cross_ids.steam_app_id": steam_appid,
            "igdb.developers": dev_list,
            "igdb.publishers": pub_list,
            "igdb.score_count": rating_count,
            "igdb.critic_score_count": agg_count,
            "igdb.score_100": score_100,
            "igdb.critic.score_100": agg_100,
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
                str(k): v for k, v in by_id.items() if (isinstance(v, dict) and "id" in v) or v is None
            }
        if isinstance(by_query, dict):
            out: dict[str, list[dict[str, Any]]] = {}
            for k, v in by_query.items():
                if isinstance(v, list):
                    out[str(k)] = [it for it in v if isinstance(it, dict)]
            self._by_query = out

    def _save_cache(self) -> None:
        self._cache_io.save_json(
            {
                "by_id": self._by_id,
                "by_query": self._by_query,
            },
            self.cache_path,
        )
