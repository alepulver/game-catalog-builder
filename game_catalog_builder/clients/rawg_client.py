from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import requests

from ..utils.utilities import (
    RateLimiter,
    extract_year_hint,
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
        self.stats: dict[str, int] = {
            "by_query_hit": 0,
            "by_query_fetch": 0,
            "by_query_negative_hit": 0,
            "by_query_negative_fetch": 0,
            "by_id_hit": 0,
            "by_id_fetch": 0,
        }
        self._by_id: dict[str, Any] = {}
        # Cache by the exact query string/params used, storing only a lightweight list of
        # candidates (id/name/released). Raw game payloads are cached by id in _by_id.
        self._by_query: dict[str, list[dict[str, Any]]] = {}
        self._load_cache(load_json_cache(self.cache_path))
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)

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
        save_json_cache(
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
        cached = self._by_id.get(id_key)
        if isinstance(cached, dict):
            self.stats["by_id_hit"] += 1
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
            self.stats["by_id_fetch"] += 1
            return data
        return None

    # ----------------------------
    # Main search
    # ----------------------------
    def search(self, game_name: str, year_hint: int | None = None) -> dict[str, Any] | None:
        def _strip_trailing_paren_year(s: str) -> str:
            y = extract_year_hint(s)
            if y is None:
                return s
            return re.sub(r"\s*\(\s*(19\d{2}|20\d{2})\s*\)\s*$", "", s).strip() or s

        stripped_name = _strip_trailing_paren_year(str(game_name or "").strip())

        search_text = stripped_name or str(game_name or "").strip()

        # RAWG can be surprisingly strict with search_exact/search_precise for some titles that
        # exist in the catalog (e.g. "Postal 4"). Try strict first, then fall back to a looser
        # query if we get 0 results.
        strict_key = (
            f"lang:{self.language}|search:{search_text}|page_size:40|search_exact:1|search_precise:1"
        )
        loose_key = f"lang:{self.language}|search:{search_text}|page_size:40"

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
                # Cache the raw payload by id so selection doesn't need to store duplicates.
                self._by_id[self._id_key(rid)] = r
            return out

        def _fetch(query_key: str, params: dict[str, Any]) -> list[dict[str, Any]] | None:
            cached = self._by_query.get(query_key)
            if cached is not None:
                self.stats["by_query_hit"] += 1
                if not cached:
                    self.stats["by_query_negative_hit"] += 1
                return cached

            def _request():
                self.ratelimiter.wait()
                r = requests.get(RAWG_API_URL, params=params, timeout=10)
                r.raise_for_status()
                return r.json()

            got = with_retries(_request, retries=3, on_fail_return=None)
            if isinstance(got, dict):
                candidates = _candidates_from_results(got.get("results") or [])
                self._by_query[query_key] = candidates
                self._save_cache()
                self.stats["by_query_fetch"] += 1
                if not candidates:
                    self.stats["by_query_negative_fetch"] += 1
                return candidates
            return None

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

        def _pick(term: str, cands: list[dict[str, Any]]):
            cands = _filter_by_numbers(cands, term)
            return pick_best_match(
                term,
                cands,
                name_key="name",
                year_hint=year_hint,
                year_getter=_year_getter,
            )

        def _search_term(
            term: str,
        ) -> tuple[dict[str, Any] | None, int, list[tuple[str, int]], bool]:
            skey = (
                f"lang:{self.language}|search:{term}|page_size:40|search_exact:1|search_precise:1"
            )
            lkey = f"lang:{self.language}|search:{term}|page_size:40"

            strict = _fetch(
                skey,
                {
                    "search": term,
                    "page_size": 40,
                    "search_exact": 1,
                    "search_precise": 1,
                    "key": self.api_key,
                    "lang": self.language,
                },
            )
            if strict is None:
                return None, 0, [], False

            cands = strict
            if not cands:
                loose = _fetch(
                    lkey,
                    {"search": term, "page_size": 40, "key": self.api_key, "lang": self.language},
                )
                cands = loose or []

            best, score, top = _pick(term, cands) if cands else (None, 0, [])
            if cands and score < 65:
                loose = _fetch(
                    lkey,
                    {"search": term, "page_size": 40, "key": self.api_key, "lang": self.language},
                )
                if isinstance(loose, list) and loose:
                    best2, score2, top2 = _pick(term, loose)
                    if score2 > score:
                        best, score, top = best2, score2, top2

            # If the candidate clearly starts with the query (common for short numbered names),
            # allow it as a match even if token_sort_ratio is low.
            if best and score < 65:
                q_norm = normalize_game_name(term)
                b_norm = normalize_game_name(str(best.get("name", "") or ""))
                if q_norm and b_norm.startswith(q_norm) and _non_year_number_tokens(term):
                    score = 65

            return best, score, top, True

        best, score, top_matches, ok = _search_term(search_text)
        if not ok:
            logging.warning(
                f"RAWG search request failed for '{game_name}' (no response); "
                "not caching as not-found."
            )
            return None

        # If still no decent match, try stripping a subtitle after ":" as a fallback.
        if score < 65 and ":" in search_text:
            base = search_text.split(":", 1)[0].strip()
            if base and base != search_text:
                best2, score2, top2, ok2 = _search_term(base)
                if score2 > score:
                    best, score, top_matches = best2, score2, top2

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
            cached = self._by_id.get(self._id_key(rawg_id))
            if isinstance(cached, dict):
                return cached
            # Fallback: fetch details if the raw payload isn't available (e.g. partial migrations).
            return self.get_by_id(rawg_id)
        return None

    def format_cache_stats(self) -> str:
        s = self.stats
        return (
            f"by_query hit={s['by_query_hit']} fetch={s['by_query_fetch']} "
            f"(neg hit={s['by_query_negative_hit']} fetch={s['by_query_negative_fetch']}), "
            f"by_id hit={s['by_id_hit']} fetch={s['by_id_fetch']}"
        )

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
