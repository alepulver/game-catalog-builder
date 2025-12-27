from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import requests

from ..config import MATCHING, RETRY, STEAM
from ..utils.utilities import (
    CacheIOTracker,
    RateLimiter,
    extract_year_hint,
    iter_chunks,
    normalize_game_name,
    pick_best_match,
)
from .http_client import ConfiguredHTTPJSONClient, HTTPJSONClient, HTTPRequestDefaults

STEAM_SEARCH_URL = "https://store.steampowered.com/api/storesearch"
STEAM_APPDETAILS_URL = "https://store.steampowered.com/api/appdetails"
STEAM_PACKAGEDETAILS_URL = "https://store.steampowered.com/api/packagedetails"


class SteamClient:
    def __init__(
        self,
        cache_path: str | Path,
        min_interval_s: float = STEAM.storesearch_min_interval_s,
    ):
        self._session = requests.Session()
        base_http = HTTPJSONClient(self._session, stats=None)
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
            "by_package_hit": 0,
            "by_package_fetch": 0,
            "by_package_negative_hit": 0,
            "by_package_negative_fetch": 0,
            # HTTP request counters (attempts, including retries).
            "http_storesearch": 0,
            "http_appdetails": 0,
            "http_packagedetails": 0,
        }
        base_http.stats = self.stats
        # Cache search results by exact query (list of {id,name,type}).
        self._by_query: dict[str, list[dict[str, Any]]] = {}
        # Cache raw appdetails payloads keyed by appid.
        self._by_id: dict[str, Any] = {}
        self._by_id_negative: set[str] = set()
        # Cache raw packagedetails payloads keyed by package/sub id.
        self._by_package: dict[str, Any] = {}
        self._by_package_negative: set[str] = set()
        self._cache_io = CacheIOTracker(self.stats)
        self._load_cache(self._cache_io.load_json(self.cache_path))
        # Steam storesearch and appdetails have different rate limits. appdetails is much stricter,
        # so we keep a slower limiter (and dynamic 429 backoff) for details.
        self.storesearch_ratelimiter = RateLimiter(min_interval_s=min_interval_s)
        self.appdetails_ratelimiter = RateLimiter(
            min_interval_s=max(min_interval_s, STEAM.appdetails_min_interval_s)
        )
        self._storesearch_http = ConfiguredHTTPJSONClient(
            base_http,
            HTTPRequestDefaults(
                ratelimiter=self.storesearch_ratelimiter,
                retries=RETRY.retries,
                counter_key="http_storesearch",
                context_prefix="Steam storesearch",
            ),
        )
        self._appdetails_http = ConfiguredHTTPJSONClient(
            base_http,
            HTTPRequestDefaults(
                ratelimiter=self.appdetails_ratelimiter,
                retries=RETRY.retries,
                base_sleep_s=max(2.0, RETRY.base_sleep_s),
                counter_key="http_appdetails",
                context_prefix="Steam appdetails",
            ),
        )
        self._packagedetails_http = ConfiguredHTTPJSONClient(
            base_http,
            HTTPRequestDefaults(
                ratelimiter=self.appdetails_ratelimiter,
                retries=RETRY.retries,
                base_sleep_s=max(2.0, RETRY.base_sleep_s),
                counter_key="http_packagedetails",
                context_prefix="Steam packagedetails",
            ),
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
        by_package = raw.get("by_package")
        by_id_negative = raw.get("by_id_negative")
        by_package_negative = raw.get("by_package_negative")
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
        if isinstance(by_package, dict):
            self._by_package = {str(k): v for k, v in by_package.items() if isinstance(v, dict)}
        if isinstance(by_id_negative, list):
            self._by_id_negative = {str(x) for x in by_id_negative if str(x).strip()}
        if isinstance(by_package_negative, list):
            self._by_package_negative = {str(x) for x in by_package_negative if str(x).strip()}

    def _save_cache(self) -> None:
        self._cache_io.save_json(
            {
                "by_id": self._by_id,
                "by_query": self._by_query,
                "by_package": self._by_package,
                "by_id_negative": sorted(self._by_id_negative),
                "by_package_negative": sorted(self._by_package_negative),
            },
            self.cache_path,
        )

    def _get_package_details(self, packageid: int) -> dict[str, Any] | None:
        if str(packageid) in self._by_package_negative:
            self.stats["by_package_negative_hit"] += 1
            return None
        cached = self._by_package.get(str(packageid))
        if isinstance(cached, dict):
            self.stats["by_package_hit"] += 1
            return cached
        url = f"{STEAM_PACKAGEDETAILS_URL}?packageids={packageid}&l=english&cc=us"
        data = self._packagedetails_http.get_json(
            url,
            context=f"packageid={packageid}",
            on_fail_return=None,
        )
        if not isinstance(data, dict):
            return None
        entry = data.get(str(packageid))
        if not isinstance(entry, dict):
            return None
        if not entry.get("success"):
            self._by_package_negative.add(str(packageid))
            self._save_cache()
            self.stats["by_package_negative_fetch"] += 1
            return None
        payload = entry.get("data")
        if not isinstance(payload, dict):
            self._by_package_negative.add(str(packageid))
            self._save_cache()
            self.stats["by_package_negative_fetch"] += 1
            return None
        self._by_package[str(packageid)] = payload
        self._save_cache()
        self.stats["by_package_fetch"] += 1
        return payload

    # -------------------------------------------------
    # Search AppID by name
    # -------------------------------------------------
    def search_appid(self, game_name: str, year_hint: int | None = None) -> dict[str, Any] | None:
        def _strip_trailing_paren_year(s: str) -> str:
            y = extract_year_hint(s)
            if y is None:
                return s
            return re.sub(r"\s*\(\s*(19\d{2}|20\d{2})\s*\)\s*$", "", s).strip() or s

        def _strip_common_editions_for_search(s: str) -> str:
            # Steam storesearch can return "sub" results for edition strings (which are not appids).
            # Strip common edition suffixes to improve the odds of finding the base app.
            out = s
            patterns = [
                r"\bgame\s+of\s+the\s+year\s+edition\b",
                r"\bgoty\s+edition\b",
                r"\bcomplete\s+edition\b",
                r"\bultimate\s+edition\b",
                r"\bdefinitive\s+edition\b",
                r"\bdeluxe\s+edition\b",
                r"\bcollector'?s\s+edition\b",
                r"\bgold\s+edition\b",
            ]
            for pat in patterns:
                out = re.sub(pat, "", out, flags=re.IGNORECASE).strip()
            out = re.sub(r"\s{2,}", " ", out).strip()
            return out or s

        stripped_name = _strip_trailing_paren_year(str(game_name or "").strip())
        base_name = _strip_common_editions_for_search(stripped_name)
        search_terms: list[str] = (
            [stripped_name] if stripped_name else [str(game_name or "").strip()]
        )
        if base_name and base_name not in search_terms:
            search_terms.append(base_name)
        if str(game_name or "").strip() and str(game_name or "").strip() not in search_terms:
            search_terms.append(str(game_name or "").strip())
        if year_hint is not None and stripped_name:
            search_terms.append(f"{stripped_name} {int(year_hint)}")

        results: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        sub_ids: list[int] = []
        request_failed = False

        for term in search_terms:
            if not term:
                continue

            query_key = f"l:english|cc:US|term:{term}"
            cached_items = self._by_query.get(query_key)
            if cached_items is None:
                data = self._storesearch_http.get_json(
                    STEAM_SEARCH_URL,
                    params={
                        "term": term,
                        "l": "english",
                        "cc": "US",
                    },
                    context=f"term={term!r}",
                    on_fail_return=None,
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
                it_type = str(it.get("type", "") or "").strip().lower()
                # Ignore non-app storesearch results; appdetails only works with real appids.
                if it_type and it_type not in {"app", "game"}:
                    if it_type == "sub":
                        try:
                            sub_ids.append(int(str(it.get("id") or "").strip()))
                        except ValueError:
                            pass
                    continue
                it_id = str(it.get("id") or "").strip()
                if not it_id or it_id in seen_ids:
                    continue
                seen_ids.add(it_id)
                results.append(it)

            # If we got any items, stop trying more terms.
            if results:
                break

        if not results and sub_ids and not request_failed:
            # Steam storesearch sometimes returns a package/subscription id (type=sub) for
            # edition strings (GOTY/Complete/etc). Those are not appids and cannot be used with
            # appdetails; try to resolve them to their underlying appids via packagedetails.
            appids: list[int] = []
            for pid in sub_ids[:10]:
                pkg = self._get_package_details(pid)
                if not isinstance(pkg, dict):
                    continue
                apps = pkg.get("apps") or []
                if not isinstance(apps, list):
                    continue
                for a in apps:
                    if not isinstance(a, dict):
                        continue
                    aid = a.get("id")
                    if aid is None:
                        continue
                    try:
                        ai = int(str(aid).strip())
                    except ValueError:
                        continue
                    appids.append(ai)
            # Dedup while preserving order
            seen = set()
            appids = [a for a in appids if not (a in seen or seen.add(a))]
            if appids:
                details_by_id = self.get_app_details_many(
                    appids[: STEAM.appdetails_refine_candidates]
                )
                detail_candidates: list[dict[str, Any]] = []
                for appid, details in details_by_id.items():
                    if not isinstance(details, dict):
                        continue
                    if str(details.get("type", "") or "").strip().lower() != "game":
                        continue
                    detail_candidates.append(
                        {
                            "id": appid,
                            "name": str(details.get("name", "") or ""),
                            "_details": details,
                        }
                    )

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
                    query = stripped_name or str(game_name or "").strip()
                    best, score, top_matches = pick_best_match(
                        query,
                        detail_candidates,
                        name_key="name",
                        year_hint=year_hint,
                        year_getter=_details_year_getter,
                    )
                    if best and score >= MATCHING.min_score:
                        if score < 100:
                            msg = (
                                f"Close match for '{game_name}': Selected '{best.get('name', '')}' "
                                f"(score: {score}%)"
                            )
                            if top_matches:
                                top_names = [f"'{name}' ({s}%)" for name, s in top_matches[:5]]
                                msg += f", alternatives: {', '.join(top_names)}"
                            logging.warning(msg)
                        return best

        if not results:
            if request_failed:
                logging.warning(
                    f"Steam search request failed for '{game_name}' (no response); "
                    "not caching as not-found."
                )
                return None
            logging.warning(f"Not found on Steam: '{game_name}'. No results from API.")
            return None

        # Prefer exact normalized title matches when present. This avoids common traps like:
        # - "Diablo" -> "Diablo IV"
        # - "Assassin's Creed" -> "Assassin's Creed Unity"
        def _norm(s: str) -> str:
            return normalize_game_name(str(s or "")).strip()

        query_norm = _norm(stripped_name or str(game_name or "").strip())
        exact = [it for it in results if _norm(str(it.get("name", "") or "")) == query_norm]
        if exact and len(exact) < len(results):
            results = exact
        elif base_name:
            base_norm = _norm(base_name)
            exact_base = [it for it in results if _norm(str(it.get("name", "") or "")) == base_norm]
            if exact_base and len(exact_base) < len(results):
                results = exact_base

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
        # typed as an app (Steam uses type=app|sub|bundle; only appids work with appdetails).
        if not query_dlc_like:

            def _store_type(it: dict[str, Any]) -> str:
                return str(it.get("type", "") or "").strip().lower()

            app_like = [it for it in results if _store_type(it) in {"app", "", "game"}]
            # Only drop sub/bundle when we have any app-like alternatives.
            if app_like and len(app_like) < len(results):
                results = app_like

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
        best_type = selected_type
        best_appid: int | None
        try:
            best_appid = int(str((best or {}).get("id") or "").strip())
        except ValueError:
            best_appid = None

        # Steam storesearch can return non-app IDs (type=sub/bundle). Those cannot be resolved via
        # appdetails and should never be pinned.
        selected_is_non_app = bool(best_type and best_type not in {"app", "game"})
        selected_valid_game: bool | None = None
        if best_appid is not None and not query_dlc_like and not selected_is_non_app:
            selected_details = self.get_app_details(best_appid)
            if isinstance(selected_details, dict):
                details_type = str(selected_details.get("type", "") or "").strip().lower()
                details_name = str(selected_details.get("name", "") or "").strip()
                selected_valid_game = details_type == "game" and not (
                    details_name and _looks_dlc_like(details_name)
                )
            else:
                selected_valid_game = False

        suspicious = (
            year_hint is not None
            or score < MATCHING.suspicious_score
            or (selected_name and _looks_dlc_like(selected_name) and not query_dlc_like)
            or (selected_is_non_app and not query_dlc_like)
            or (selected_valid_game is False)
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
                    c
                    for c in detail_candidates
                    if not _looks_dlc_like(str(c.get("name", "") or ""))
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
                if best2 is not None and score2 >= MATCHING.min_score:
                    must_replace = bool(selected_is_non_app or selected_valid_game is False)
                    if must_replace or score2 >= max(score, MATCHING.min_score):
                        best, score, top_matches = best2, score2, top2

        # Final guard: never pin non-app Steam storesearch results (subs/bundles).
        if best and not query_dlc_like:
            final_type = str(best.get("type", "") or "").strip().lower()
            if final_type and final_type not in {"app", "game"}:
                logging.warning(
                    f"Not found on Steam: '{game_name}'. Selected storesearch type is not an app: "
                    f"id={best.get('id')} type={final_type!r}"
                )
                return None
            try:
                final_appid = int(str(best.get("id") or "").strip())
            except ValueError:
                final_appid = None
            if final_appid is not None:
                final_details = self.get_app_details(final_appid)
                if not isinstance(final_details, dict):
                    logging.warning(
                        f"Not found on Steam: '{game_name}'. "
                        "Selected Steam ID could not be resolved via appdetails. "
                        f"appid={final_appid}"
                    )
                    return None
                final_details_type = str(final_details.get("type", "") or "").strip().lower()
                final_details_name = str(final_details.get("name", "") or "").strip()
                if final_details_type and final_details_type != "game":
                    logging.warning(
                        f"Not found on Steam: '{game_name}'. Selected appdetails type is not game: "
                        f"appid={final_appid} type={final_details_type!r}"
                    )
                    return None
                if final_details_name and _looks_dlc_like(final_details_name):
                    logging.warning(
                        f"Not found on Steam: '{game_name}'. "
                        f"Selected title looks like DLC/non-game: '{final_details_name}'"
                    )
                    return None

        # Final guard: if we still ended up selecting a DLC-like title but the query isn't
        # DLC-like, treat it as not found rather than pinning the wrong Steam app.
        if best and not query_dlc_like:
            chosen = str(best.get("name", "") or "").strip()
            if chosen and _looks_dlc_like(chosen):
                logging.warning(
                    f"Not found on Steam: '{game_name}'. "
                    f"Selected title looks like DLC/non-game: '{chosen}'"
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
            if str(appid) in self._by_id_negative:
                self.stats["by_id_negative_hit"] += 1
                continue
            cached = self._by_id.get(str(appid))
            if isinstance(cached, dict):
                self.stats["by_id_hit"] += 1
                out[appid] = cached
            else:
                missing.append(appid)

        def _fetch_chunk(chunk: list[int]) -> dict[str, Any] | None:
            ids = ",".join(str(i) for i in chunk)
            # Steam's appdetails endpoint does not reliably accept URL-encoded commas in the
            # `appids` query param. Build the query string manually to keep commas literal.
            url = f"{STEAM_APPDETAILS_URL}?appids={ids}&l=english&cc=us"
            got = self._appdetails_http.get_json(
                url,
                status_handlers={400: {"__status": 400}},
                context=f"appids={ids}",
                on_fail_return=None,
            )
            if not isinstance(got, dict):
                return None
            status = got.get("__status")
            if status == 400:
                if len(chunk) > 1:
                    merged: dict[str, Any] = {}
                    for single in chunk:
                        one = _fetch_chunk([single])
                        if isinstance(one, dict):
                            merged.update(one)
                    return merged
                logging.warning(f"Steam appdetails rejected appid={ids} (HTTP 400); skipping")
                return {}
            return got if status is None else None

        # Fetch missing IDs in small chunks to keep response sizes reasonable.
        wrote_negative = False
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
                    self._by_id_negative.add(str(appid))
                    wrote_negative = True
                    self.stats["by_id_negative_fetch"] += 1
                    continue
                details = entry.get("data")
                if not isinstance(details, dict):
                    self._by_id_negative.add(str(appid))
                    wrote_negative = True
                    self.stats["by_id_negative_fetch"] += 1
                    continue
                self._by_id[str(appid)] = details
                out[appid] = details
                self.stats["by_id_fetch"] += 1

        if missing or wrote_negative:
            self._save_cache()
        return out

    def get_app_details(self, appid: int) -> dict[str, Any] | None:
        return self.get_app_details_many([appid]).get(appid)

    def format_cache_stats(self) -> str:
        s = self.stats
        base = (
            f"by_query hit={s['by_query_hit']} fetch={s['by_query_fetch']} "
            f"(neg hit={s['by_query_negative_hit']} fetch={s['by_query_negative_fetch']}), "
            f"by_id hit={s['by_id_hit']} fetch={s['by_id_fetch']} "
            f"(neg hit={s['by_id_negative_hit']} fetch={s['by_id_negative_fetch']}), "
            f"by_package hit={s['by_package_hit']} fetch={s['by_package_fetch']} "
            f"(neg hit={s['by_package_negative_hit']} fetch={s['by_package_negative_fetch']}), "
            f"{HTTPJSONClient.format_timing(s, key='http_storesearch')} "
            f"{HTTPJSONClient.format_timing(s, key='http_appdetails')} "
            f"{HTTPJSONClient.format_timing(s, key='http_packagedetails')}"
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
        metacritic = details.get("metacritic", {}) or {}
        metacritic_score = metacritic.get("score", "")
        store_url = ""
        try:
            if str(appid).isdigit():
                store_url = f"https://store.steampowered.com/app/{int(appid)}/"
        except Exception:
            store_url = ""
        store_type = str(details.get("type", "") or "").strip().lower()

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

        developers = details.get("developers", []) or []
        publishers = details.get("publishers", []) or []
        if isinstance(developers, str):
            developers = [developers]
        if isinstance(publishers, str):
            publishers = [publishers]

        def _split_listish_company_string(value: str) -> list[str]:
            """
            Steam sometimes returns a single developer string containing multiple studios,
            e.g. "Ubisoft Montreal, Massive Entertainment, and Ubisoft Shanghai".
            Keep this conservative to avoid splitting legitimate company names.
            """
            s = str(value or "").strip()
            if not s:
                return []
            # Avoid splitting porting labels like "Aspyr (Mac, Linux)".
            if "(" in s or ")" in s:
                return [s]
            # Only split when it clearly looks like a list (2+ commas).
            if s.count(",") < 2:
                return [s]
            parts = [p.strip() for p in s.split(",") if p.strip()]
            parts = [re.sub(r"(?i)^and\s+", "", p).strip() for p in parts]
            # Special-case Ubisoft location lists like "... Red Storm, Shanghai, Toronto, Kiev"
            # without inventing new entities for non-location names.
            if parts and parts[0].casefold().startswith("ubisoft "):
                ubisoft_locations = {
                    "shanghai",
                    "toronto",
                    "kiev",
                    "kyiv",
                    "quebec",
                    "québec",
                    "montreal",
                    "montréal",
                }
                out: list[str] = []
                for p in parts:
                    if p.casefold().startswith("ubisoft "):
                        out.append(p)
                        continue
                    # If it's a 1-token location, prefix with Ubisoft.
                    if p.casefold() in ubisoft_locations:
                        out.append(f"Ubisoft {p}")
                    else:
                        out.append(p)
                return [x for x in out if x]
            return [p for p in parts if p]

        dev_list: list[str] = []
        if isinstance(developers, list):
            for x in developers:
                for v in _split_listish_company_string(str(x).strip()):
                    if v and v not in dev_list:
                        dev_list.append(v)

        pub_list = (
            [str(x).strip() for x in (publishers or []) if str(x).strip()]
            if isinstance(publishers, list)
            else []
        )
        dev_str = json.dumps(dev_list, ensure_ascii=False)
        pub_str = json.dumps(pub_list, ensure_ascii=False)

        return {
            "Steam_AppID": str(appid),
            "Steam_Name": str(details.get("name", "") or ""),
            "Steam_URL": store_url,
            "Steam_Website": str(details.get("website", "") or "").strip(),
            "Steam_ShortDescription": str(details.get("short_description", "") or "").strip(),
            "Steam_StoreType": store_type,
            "Steam_ReleaseYear": release_year,
            "Steam_Platforms": ", ".join(platform_names),
            "Steam_Tags": ", ".join(genres),
            "Steam_ReviewCount": str(review_count),
            "Steam_Price": price,
            "Steam_Categories": ", ".join(categories),
            "Steam_Metacritic": str(metacritic_score),
            "Steam_Developers": dev_str,
            "Steam_Publishers": pub_str,
        }
