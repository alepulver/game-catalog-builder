from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import requests

from ..config import MATCHING, RAWG, RETRY
from .http_client import ConfiguredHTTPJSONClient, HTTPJSONClient, HTTPRequestDefaults
from ..utils.utilities import (
    CacheIOTracker,
    RateLimiter,
    extract_year_hint,
    normalize_game_name,
    pick_best_match,
)

RAWG_API_URL = "https://api.rawg.io/api/games"


class RAWGClient:
    def __init__(
        self,
        api_key: str,
        cache_path: str | Path,
        language: str = "en",
        min_interval_s: float = RAWG.min_interval_s,
    ):
        self._session = requests.Session()
        base_http = HTTPJSONClient(self._session, stats=None)
        self.api_key = api_key
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
            "http_get": 0,
        }
        base_http.stats = self.stats
        self._http = ConfiguredHTTPJSONClient(
            base_http,
            HTTPRequestDefaults(
                ratelimiter=None,  # set after RateLimiter init
                retries=RETRY.retries,
                counter_key="http_get",
                context_prefix="RAWG",
            ),
        )
        self._by_id: dict[str, Any] = {}
        # Cache by the exact query string/params used, storing only a lightweight list of
        # candidates (id/name/released). Raw game payloads are cached by id in _by_id.
        self._by_query: dict[str, list[dict[str, Any]]] = {}
        self._cache_io = CacheIOTracker(self.stats)
        self._load_cache(self._cache_io.load_json(self.cache_path))
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)
        self._http.defaults.ratelimiter = self.ratelimiter

    def _id_key(self, rawg_id: int | str) -> str:
        return f"{self.language}:{rawg_id}"

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return

        by_id = raw.get("by_id")
        by_query = raw.get("by_query")
        if not isinstance(by_id, dict):
            logging.warning(
                "RAWG cache file is in an incompatible format; ignoring it (delete it to rebuild)."
            )
            return
        self._by_id = {str(k): v for k, v in by_id.items()}
        if isinstance(by_query, dict):
            out: dict[str, list[dict[str, Any]]] = {}
            for k, v in by_query.items():
                if isinstance(v, list):
                    out[str(k)] = v
            self._by_query = out

    def _save_cache(self) -> None:
        self._cache_io.save_json(
            {
                "by_id": self._by_id,
                "by_query": self._by_query,
            },
            self.cache_path,
        )

    def get_by_id(self, rawg_id: int | str) -> dict[str, Any] | None:
        """
        Fetch a RAWG game by id (preferring cache).
        """
        rawg_id_str = str(rawg_id).strip()
        if not rawg_id_str:
            return None

        id_key = self._id_key(rawg_id_str)
        if id_key in self._by_id and self._by_id.get(id_key) is None:
            self.stats["by_id_negative_hit"] += 1
            return None
        cached = self._by_id.get(id_key)
        if isinstance(cached, dict):
            self.stats["by_id_hit"] += 1
            return cached
        data = self._http.get_json(
            f"{RAWG_API_URL}/{rawg_id_str}",
            params={"key": self.api_key, "lang": self.language},
            context=f"get_by_id id={rawg_id_str}",
            on_fail_return=None,
        )
        if isinstance(data, dict) and data.get("id") is not None:
            self._by_id[id_key] = data
            self._save_cache()
            self.stats["by_id_fetch"] += 1
            return data
        # If RAWG returned a real payload but it isn't a valid game object, cache it as a
        # negative by-id result to avoid repeated fetches.
        if isinstance(data, dict):
            self._by_id[id_key] = None
            self._save_cache()
            self.stats["by_id_negative_fetch"] += 1
        return None

    # ----------------------------
    # Main search
    # ----------------------------
    @staticmethod
    def _select_best_candidate(
        *,
        query: str,
        candidates: list[dict[str, Any]],
        year_hint: int | None,
    ) -> tuple[dict[str, Any] | None, int, list[tuple[str, int]]]:
        def _year_getter(obj: dict[str, Any]) -> int | None:
            released = str(obj.get("released", "") or "").strip()
            if len(released) >= 4 and released[:4].isdigit():
                return int(released[:4])
            return None

        def _norm(s: str) -> str:
            return normalize_game_name(str(s or "")).strip()

        def _non_year_number_tokens(term: str) -> set[str]:
            toks = normalize_game_name(term).split()
            out: set[str] = set()
            for t in toks:
                if not t.isdecimal():
                    continue
                if len(t) == 4 and t[:2] in {"19", "20"}:
                    continue
                out.add(t)
            return out

        def _filter_by_numbers(cands: list[dict[str, Any]], term: str) -> list[dict[str, Any]]:
            nums = _non_year_number_tokens(term)
            if not nums:
                return cands
            term_tokens = set(normalize_game_name(term).split())
            term_words = {t for t in term_tokens if not t.isdigit()}
            filtered = []
            for c in cands:
                cname = str(c.get("name", "") or "")
                c_tokens = set(normalize_game_name(cname).split())
                cnums = _non_year_number_tokens(cname)
                if not nums.issubset(cnums):
                    continue
                if term_words and term_words.isdisjoint({t for t in c_tokens if not t.isdigit()}):
                    continue
                filtered.append(c)
            return filtered or cands

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

        q_norm = _norm(query)
        query_dlc_like = _looks_dlc_like(query)

        # Prefer exact normalized title matches when present. This avoids common traps like:
        # - "Diablo" -> "Diablo IV"
        exact = [it for it in candidates if _norm(str(it.get("name", "") or "")) == q_norm]
        if exact and len(exact) < len(candidates):
            candidates = exact

        # If the query has no sequel number, prefer candidates without explicit sequel numbers
        # when alternatives exist.
        if q_norm and not _series_numbers(q_norm):
            no_nums = [
                it
                for it in candidates
                if not _series_numbers(str(it.get("name", "") or ""))
            ]
            if no_nums and len(no_nums) < len(candidates):
                candidates = no_nums

        # Prefer to avoid DLC/demo/soundtrack-like matches unless explicitly requested.
        if not query_dlc_like:
            non_dlc = [
                it for it in candidates if not _looks_dlc_like(str(it.get("name", "") or ""))
            ]
            if non_dlc and len(non_dlc) < len(candidates):
                candidates = non_dlc

        if year_hint is not None:
            tol = int(MATCHING.year_hint_tolerance)
            near = []
            for it in candidates:
                y = _year_getter(it)
                if y is None:
                    continue
                if abs(int(y) - int(year_hint)) <= tol:
                    near.append(it)
            if near and len(near) < len(candidates):
                candidates = near

        candidates = _filter_by_numbers(candidates, query)
        return pick_best_match(
            query,
            candidates,
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

        search_text = stripped_name or str(game_name or "").strip()

        def _candidates_from_results(results: Any) -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            if not isinstance(results, list):
                return out
            for r in results:
                if not isinstance(r, dict):
                    continue
                rid = r.get("id")
                if rid is None:
                    continue
                out.append(
                    {
                        "id": rid,
                        "name": r.get("name", ""),
                        "released": r.get("released", ""),
                    }
                )
            return out

        def _fetch(query_key: str, params: dict[str, Any]) -> list[dict[str, Any]] | None:
            cached = self._by_query.get(query_key)
            if cached is not None:
                self.stats["by_query_hit"] += 1
                if not cached:
                    self.stats["by_query_negative_hit"] += 1
                return cached
            got = self._http.get_json(
                RAWG_API_URL,
                params=params,
                context=f"search term={str(params.get('search') or '')!r}",
                on_fail_return=None,
            )
            if not isinstance(got, dict):
                return None
            candidates = _candidates_from_results(got.get("results") or [])
            self._by_query[query_key] = candidates
            self._save_cache()
            self.stats["by_query_fetch"] += 1
            if not candidates:
                self.stats["by_query_negative_fetch"] += 1
            return candidates

        def _year_getter(obj: dict[str, Any]) -> int | None:
            released = str(obj.get("released", "") or "").strip()
            if len(released) >= 4 and released[:4].isdigit():
                return int(released[:4])
            return None

        def _non_year_number_tokens(term: str) -> set[str]:
            toks = normalize_game_name(term).split()
            out: set[str] = set()
            for t in toks:
                if not t.isdigit():
                    continue
                if len(t) == 4 and t[:2] in {"19", "20"}:
                    continue
                out.add(t)
            return out

        def _search_term(
            term: str,
        ) -> tuple[dict[str, Any] | None, int, list[tuple[str, int]], bool]:
            lkey = f"lang:{self.language}|search:{term}|page_size:40"

            cands = _fetch(
                lkey,
                {"search": term, "page_size": 40, "key": self.api_key, "lang": self.language},
            )
            if cands is None:
                return None, 0, [], False

            best, score, top = (
                self._select_best_candidate(query=term, candidates=cands, year_hint=year_hint)
                if cands
                else (None, 0, [])
            )

            # If the candidate clearly starts with the query (common for short numbered names),
            # allow it as a match even if token_sort_ratio is low.
            if best and score < MATCHING.min_score:
                q_norm = normalize_game_name(term)
                b_norm = normalize_game_name(str(best.get("name", "") or ""))
                if q_norm and b_norm.startswith(q_norm) and _non_year_number_tokens(term):
                    score = MATCHING.min_score

            return best, score, top, True

        best, score, top_matches, ok = _search_term(search_text)
        if not ok:
            logging.warning(
                f"RAWG search request failed for '{game_name}' (no response); "
                "not caching as not-found."
            )
            return None

        # If still no decent match, try stripping a subtitle after ":" as a fallback.
        if score < MATCHING.min_score and ":" in search_text:
            base = search_text.split(":", 1)[0].strip()
            if base and base != search_text:
                best2, score2, top2, ok2 = _search_term(base)
                if score2 > score:
                    best, score, top_matches = best2, score2, top2

        # Minimum threshold to accept the match
        if not best or score < MATCHING.min_score:
            # Log top 5 closest matches when not found
            if top_matches:
                top_names = [f"'{name}' ({s}%)" for name, s in top_matches[:5]]
                logging.warning(
                    f"Not found in RAWG: '{game_name}'. Closest matches: {', '.join(top_names)}"
                )
            else:
                logging.warning(f"Not found in RAWG: '{game_name}'. No matches found.")
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
            # Always go through get_by_id() so the cache is populated with the full RAWG game
            # detail payload (search results are partial and omit fields like descriptions and
            # alternative_names).
            return self.get_by_id(rawg_id)
        return None

    def format_cache_stats(self) -> str:
        s = self.stats
        base = (
            f"by_query hit={s['by_query_hit']} fetch={s['by_query_fetch']} "
            f"(neg hit={s['by_query_negative_hit']} fetch={s['by_query_negative_fetch']}), "
            f"by_id hit={s['by_id_hit']} fetch={s['by_id_fetch']} "
            f"(neg hit={s['by_id_negative_hit']} fetch={s['by_id_negative_fetch']}), "
            f"{HTTPJSONClient.format_timing(s, key='http_get')}"
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

    # ----------------------------
    # Metadata extraction
    # ----------------------------
    @staticmethod
    def extract_fields(rawg_obj: dict[str, Any]) -> dict[str, str]:
        if not rawg_obj:
            return {}

        def _truncate(text: object, max_len: int = 500) -> str:
            s = str(text or "").strip()
            if not s:
                return ""
            if len(s) <= max_len:
                return s
            return s[:max_len].rstrip() + "…"

        genres: list[str] = []
        for g in rawg_obj.get("genres", []) or []:
            if not isinstance(g, dict):
                continue
            n = str(g.get("name") or "").strip()
            if n:
                genres.append(n)
        platforms = [p.get("platform", {}).get("name", "") for p in rawg_obj.get("platforms", [])]
        tags = [t.get("name", "") for t in rawg_obj.get("tags", [])]
        # RAWG tags can contain mixed-language duplicates; drop Cyrillic tags by default.
        tags = [t for t in tags if t and not re.search(r"[А-Яа-яЁё]", t)]

        released = rawg_obj.get("released") or ""
        website = str(rawg_obj.get("website", "") or "").strip()
        desc_raw = _truncate(rawg_obj.get("description_raw", ""))
        esrb = str((rawg_obj.get("esrb_rating") or {}).get("name") or "").strip()

        rating_val = rawg_obj.get("rating", None)
        score_100 = ""
        try:
            if rating_val is not None:
                score_100 = str(int(round(float(rating_val) / 5.0 * 100.0)))
        except Exception:
            score_100 = ""

        devs = [
            d.get("name", "")
            for d in (rawg_obj.get("developers", []) or [])
            if isinstance(d, dict)
        ]
        pubs = [
            p.get("name", "")
            for p in (rawg_obj.get("publishers", []) or [])
            if isinstance(p, dict)
        ]
        dev_list = [str(x).strip() for x in devs if str(x).strip()]
        pub_list = [str(x).strip() for x in pubs if str(x).strip()]

        return {
            "RAWG_ID": str(rawg_obj.get("id", "")),
            "RAWG_Name": str(rawg_obj.get("name", "") or ""),
            "RAWG_Released": str(released or ""),
            "RAWG_Year": released[:4] if released else "",
            "RAWG_Website": website,
            "RAWG_DescriptionRaw": desc_raw,
            "RAWG_Genre": genres[0] if len(genres) > 0 else "",
            "RAWG_Genre2": genres[1] if len(genres) > 1 else "",
            "RAWG_Genres": ", ".join(genres),
            "RAWG_Platforms": ", ".join(p for p in platforms if p),
            "RAWG_Tags": ", ".join(t for t in tags if t),
            "RAWG_ESRB": esrb,
            "RAWG_Rating": str(rating_val if rating_val is not None else ""),
            "Score_RAWG_100": score_100,
            "RAWG_RatingsCount": str(rawg_obj.get("ratings_count", "")),
            "RAWG_Metacritic": str(rawg_obj.get("metacritic", "")),
            "RAWG_Developers": json.dumps(dev_list, ensure_ascii=False),
            "RAWG_Publishers": json.dumps(pub_list, ensure_ascii=False),
        }
