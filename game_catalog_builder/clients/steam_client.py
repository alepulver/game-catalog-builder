from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from typing import Any

import requests

from ..config import MATCHING, REQUEST, RETRY, STEAM
from ..utils.utilities import (
    RateLimiter,
    extract_year_hint,
    iter_chunks,
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
        min_interval_s: float = STEAM.storesearch_min_interval_s,
    ):
        self.cache_path = Path(cache_path)
        self.stats: dict[str, int] = {
            "by_query_hit": 0,
            "by_query_fetch": 0,
            "by_query_negative_hit": 0,
            "by_query_negative_fetch": 0,
            "by_id_hit": 0,
            "by_id_fetch": 0,
        }
        # Cache search results by exact query (list of {id,name,type}).
        self._by_query: dict[str, list[dict[str, Any]]] = {}
        # Cache raw appdetails payloads keyed by appid.
        self._by_id: dict[str, Any] = {}
        self._load_cache(load_json_cache(self.cache_path))
        # Steam storesearch and appdetails have different rate limits. appdetails is much stricter,
        # so we keep a slower limiter (and dynamic 429 backoff) for details.
        self.storesearch_ratelimiter = RateLimiter(min_interval_s=min_interval_s)
        self.appdetails_ratelimiter = RateLimiter(
            min_interval_s=max(min_interval_s, STEAM.appdetails_min_interval_s)
        )

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return

        def _looks_like_appdetails(v: Any) -> bool:
            if not isinstance(v, dict):
                return False
            # Legacy caches sometimes stored storesearch items under by_id; those are small dicts
            # like {id,name,type}. Appdetails payloads include richer metadata.
            if "release_date" in v or "platforms" in v or "developers" in v or "publishers" in v:
                return True
            if "steam_appid" in v or "categories" in v or "genres" in v:
                return True
            if "is_free" in v or "price_overview" in v:
                return True
            return False

        by_query = raw.get("by_query")
        by_id = raw.get("by_id")
        if not isinstance(by_id, dict):
            logging.warning(
                "Steam cache file is in an incompatible format; ignoring it (delete it to rebuild)."
            )
            return
        # by_id must contain appdetails payloads only.
        self._by_id = {str(k): v for k, v in by_id.items() if _looks_like_appdetails(v)}

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

    # -------------------------------------------------
    # Search AppID by name
    # -------------------------------------------------
    def search_appid(self, game_name: str, year_hint: int | None = None) -> dict[str, Any] | None:
        def _strip_trailing_paren_year(s: str) -> str:
            y = extract_year_hint(s)
            if y is None:
                return s
            return re.sub(r"\s*\(\s*(19\d{2}|20\d{2})\s*\)\s*$", "", s).strip() or s

        stripped_name = _strip_trailing_paren_year(str(game_name or "").strip())
        search_terms: list[str] = (
            [stripped_name] if stripped_name else [str(game_name or "").strip()]
        )
        if str(game_name or "").strip() and str(game_name or "").strip() not in search_terms:
            search_terms.append(str(game_name or "").strip())
        if year_hint is not None and stripped_name:
            search_terms.append(f"{stripped_name} {int(year_hint)}")

        results: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        request_failed = False

        for term in search_terms:
            if not term:
                continue

            query_key = f"l:english|cc:US|term:{term}"
            cached_items = self._by_query.get(query_key)
            if cached_items is None:
                def _request(term=term):
                    self.storesearch_ratelimiter.wait()
                    r = requests.get(
                        STEAM_SEARCH_URL,
                        params={
                            "term": term,
                            "l": "english",
                            "cc": "US",
                        },
                        timeout=REQUEST.timeout_s,
                    )
                    r.raise_for_status()
                    return r.json()

                data = with_retries(
                    _request,
                    retries=RETRY.retries,
                    on_fail_return=None,
                    context=f"Steam storesearch term={term!r}",
                )
                if isinstance(data, dict):
                    cached_items = [it for it in (data.get("items") or []) if isinstance(it, dict)]
                    # Cache empty results too (negative cache) by query key.
                    self._by_query[query_key] = cached_items
                    self._save_cache()
                    self.stats["by_query_fetch"] += 1
                    if not cached_items:
                        self.stats["by_query_negative_fetch"] += 1
            else:
                self.stats["by_query_hit"] += 1
                if not cached_items:
                    self.stats["by_query_negative_hit"] += 1

            if cached_items is None:
                request_failed = True
                continue
            for it in cached_items:
                if not isinstance(it, dict):
                    continue
                it_id = str(it.get("id") or "").strip()
                if not it_id or it_id in seen_ids:
                    continue
                seen_ids.add(it_id)
                results.append(it)

            # If we got any items, stop trying more terms.
            if results:
                break

        if not results:
            if request_failed:
                logging.warning(
                    f"Steam search request failed for '{game_name}' (no response); "
                    "not caching as not-found."
                )
                return None
            logging.warning(f"Not found on Steam: '{game_name}'. No results from API.")
            return None

        # If the query has no sequel number, prefer candidates that are the same base game name
        # plus "edition" tokens (GOTY/Complete/etc) over candidates with explicit sequel numbers.
        def _strip_edition_tokens(name: str) -> str:
            # Keep this set small and conservative; it's only for Steam storesearch selection.
            edition = {
                "remake",
                "hd",
                "classic",
                "definitive",
                "remastered",
                "ultimate",
                "goty",
                "anniversary",
                "complete",
                "collection",
                "edition",
                "enhanced",
                "redux",
                "vr",
                "directors",
                "director",
                "cut",
                "story",
                "game",
                "of",
                "the",
                "year",
                "deluxe",
            }
            toks = [t for t in normalize_game_name(name).split() if t not in edition]
            return " ".join(toks).strip()

        def _has_series_number(name: str) -> bool:
            toks = normalize_game_name(name).split()
            for t in toks:
                if t.isdigit() and len(t) <= 2 and t not in {"0", "1"}:
                    return True
            return False

        def _series_numbers(name: str) -> set[int]:
            toks = normalize_game_name(name).split()
            out: set[int] = set()
            for t in toks:
                if not t.isdigit():
                    continue
                if len(t) > 1 and t.startswith("0"):
                    continue
                n = int(t)
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

        q_stripped = _strip_edition_tokens(stripped_name or str(game_name or "").strip())
        preferred = []
        if q_stripped and not _has_series_number(q_stripped):
            for it in results:
                nm = str(it.get("name", "") or "")
                if not nm:
                    continue
                if _strip_edition_tokens(nm) == q_stripped and not _has_series_number(nm):
                    preferred.append(it)

            # Only apply the preference when it actually narrows candidates.
            if preferred and len(preferred) < len(results):
                results = preferred

        def _year_getter(obj: dict[str, Any]) -> int | None:
            # Steam storesearch doesn't expose release year; only use a year embedded in the title
            # as a soft hint.
            m = re.search(r"\b(19\d{2}|20\d{2})\b", str(obj.get("name", "") or ""))
            if m:
                try:
                    return int(m.group(1))
                except ValueError:
                    return None
            return None

        query = stripped_name or str(game_name or "").strip()
        query_dlc_like = _looks_dlc_like(query)

        # If the user did not ask for DLC-like content, prefer Steam storesearch results that are
        # typed as a game (when the endpoint provides a type).
        if not query_dlc_like:
            typed_games = [
                it
                for it in results
                if str(it.get("type", "") or "").strip().lower() in {"game", ""}
            ]
            if typed_games and len(typed_games) < len(results):
                results = typed_games

            # Also drop obviously DLC-like titles when we have any non-DLC-like alternatives.
            non_dlc_like = [
                it for it in results if not _looks_dlc_like(str(it.get("name", "") or ""))
            ]
            if non_dlc_like and len(non_dlc_like) < len(results):
                results = non_dlc_like

        best, score, top_matches = pick_best_match(
            query,
            results,
            name_key="name",
            year_hint=year_hint,
            year_getter=_year_getter,
        )

        # If we have a year hint OR the initial selection is suspicious, prefer selecting among
        # candidates using appdetails (type + release year). Steam storesearch doesn't expose
        # release year and can surface DLC/soundtrack/demo entries that have deceptively close
        # names, so appdetails is the only reliable way to reject non-game types.
        selected_name = str((best or {}).get("name", "") or "").strip()
        selected_type = str((best or {}).get("type", "") or "").strip().lower()
        suspicious = (
            year_hint is not None
            or score < MATCHING.suspicious_score
            or (selected_name and _looks_dlc_like(selected_name) and not query_dlc_like)
            or (selected_type and selected_type not in {"game"} and not query_dlc_like)
            or (_series_numbers(query) != _series_numbers(selected_name))
        )
        if suspicious:
            detail_candidates: list[dict[str, Any]] = []
            # appdetails is significantly more expensive than storesearch and is subject to much
            # tighter throttling; only sample a few top candidates.
            sampled_appids: list[int] = []
            for it in results[: STEAM.appdetails_refine_candidates]:
                it_id = it.get("id")
                if it_id is None:
                    continue
                try:
                    sampled_appids.append(int(str(it_id)))
                except ValueError:
                    continue

            details_by_id = self.get_app_details_many(sampled_appids)

            # If the initially-selected storesearch item is a DLC/non-game in appdetails, reject
            # it even if the storesearch title doesn't include DLC-like tokens.
            if best and not query_dlc_like:
                try:
                    selected_appid = int(str(best.get("id") or "").strip())
                except ValueError:
                    selected_appid = None
                if selected_appid is not None:
                    selected_details = details_by_id.get(selected_appid)
                    if isinstance(selected_details, dict):
                        details_type = str(selected_details.get("type", "") or "").strip().lower()
                        details_name = str(selected_details.get("name", "") or "").strip()
                        if details_type and details_type != "game":
                            logging.warning(
                                f"Not found on Steam: '{game_name}'. Selected appdetails type is not game: "
                                f"appid={selected_appid} type={details_type!r}"
                            )
                            return None
                        if details_name and _looks_dlc_like(details_name):
                            logging.warning(
                                f"Not found on Steam: '{game_name}'. Selected title looks like DLC/non-game: '{details_name}'"
                            )
                            return None

            for appid in sampled_appids:
                details = details_by_id.get(appid)
                if not isinstance(details, dict):
                    continue
                details_type = str(details.get("type", "") or "").strip().lower()
                if details_type != "game":
                    continue
                detail_candidates.append(
                    {"id": appid, "name": str(details.get("name", "") or ""), "_details": details}
                )

            if not query_dlc_like:
                detail_candidates = [
                    c for c in detail_candidates if not _looks_dlc_like(str(c.get("name", "") or ""))
                ]

            def _details_year_getter(obj: dict[str, Any]) -> int | None:
                details = obj.get("_details") if isinstance(obj, dict) else None
                if not isinstance(details, dict):
                    return None
                release = details.get("release_date", {}) or {}
                text = str(release.get("date", "") or "")
                m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
                if m:
                    try:
                        return int(m.group(1))
                    except ValueError:
                        return None
                return None

            if detail_candidates:
                best2, score2, top2 = pick_best_match(
                    query,
                    detail_candidates,
                    name_key="name",
                    year_hint=year_hint,
                    year_getter=_details_year_getter,
                )
                if best2 is not None and score2 >= max(score, MATCHING.min_score):
                    best, score, top_matches = best2, score2, top2

        # Final guard: if we still ended up selecting a DLC-like title but the query isn't
        # DLC-like, treat it as not found rather than pinning the wrong Steam app.
        if best and not query_dlc_like:
            chosen = str(best.get("name", "") or "").strip()
            if chosen and _looks_dlc_like(chosen):
                logging.warning(
                    f"Not found on Steam: '{game_name}'. Selected title looks like DLC/non-game: '{chosen}'"
                )
                return None

        if not best or score < MATCHING.min_score:
            # Log top 5 closest matches when not found
            if top_matches:
                top_names = [f"'{name}' ({s}%)" for name, s in top_matches[:5]]
                logging.warning(
                    f"Not found on Steam: '{game_name}'. Closest matches: {', '.join(top_names)}"
                )
            else:
                logging.warning(f"Not found on Steam: '{game_name}'. No matches found.")
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
        return best

    # -------------------------------------------------
    # Game details
    # -------------------------------------------------
    def get_app_details_many(self, appids: list[int]) -> dict[int, dict[str, Any]]:
        """
        Fetch Steam appdetails for multiple appids in one request (when possible).

        Returns a mapping of appid -> details dict for successful responses.
        """
        out: dict[int, dict[str, Any]] = {}
        missing: list[int] = []
        for appid in appids:
            cached = self._by_id.get(str(appid))
            if isinstance(cached, dict):
                self.stats["by_id_hit"] += 1
                out[appid] = cached
            else:
                missing.append(appid)

        def _fetch_chunk(chunk: list[int]) -> dict[str, Any] | None:
            ids = ",".join(str(i) for i in chunk)

            def _request():
                self.appdetails_ratelimiter.wait()
                r = requests.get(
                    STEAM_APPDETAILS_URL,
                    params={"appids": ids, "l": "english", "cc": "us"},
                    timeout=REQUEST.timeout_s,
                )
                r.raise_for_status()
                return r.json()

            return with_retries(
                _request,
                retries=RETRY.retries,
                base_sleep_s=max(2.0, RETRY.base_sleep_s),
                on_fail_return=None,
                context=f"Steam appdetails appids={ids}",
            )

        # Fetch missing IDs in small chunks to keep response sizes reasonable.
        for chunk in iter_chunks(missing, STEAM.appdetails_batch_size):
            data = _fetch_chunk(chunk)
            if not isinstance(data, dict):
                continue
            for appid in chunk:
                entry = data.get(str(appid))
                if not isinstance(entry, dict):
                    continue
                if not entry.get("success"):
                    logging.warning(f"Steam appdetails returned success=false for appid={appid}")
                    continue
                details = entry.get("data")
                if not isinstance(details, dict):
                    continue
                self._by_id[str(appid)] = details
                out[appid] = details
                self.stats["by_id_fetch"] += 1

        if missing:
            self._save_cache()
        return out

    def get_app_details(self, appid: int) -> dict[str, Any] | None:
        return self.get_app_details_many([appid]).get(appid)

    def format_cache_stats(self) -> str:
        s = self.stats
        return (
            f"by_query hit={s['by_query_hit']} fetch={s['by_query_fetch']} "
            f"(neg hit={s['by_query_negative_hit']} fetch={s['by_query_negative_fetch']}), "
            f"by_id hit={s['by_id_hit']} fetch={s['by_id_fetch']}"
        )

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
