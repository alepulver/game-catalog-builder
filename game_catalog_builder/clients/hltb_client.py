from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

from howlongtobeatpy import HowLongToBeat

from ..config import HLTB
from ..utils.utilities import CacheIOTracker, extract_year_hint, fuzzy_score


class HLTBClient:
    def __init__(self, cache_path: str | Path):
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
        self._by_id: dict[str, Any] = {}
        # Cache query -> lightweight candidates (id/name). Raw game payloads are cached by id.
        self._by_query: dict[str, list[dict[str, Any]]] = {}
        self._cache_io = CacheIOTracker(self.stats)
        self._load_cache(self._cache_io.load_json(self.cache_path))
        self.client = HowLongToBeat()

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, list):
            return [HLTBClient._json_safe(v) for v in value]
        if isinstance(value, tuple):
            return [HLTBClient._json_safe(v) for v in value]
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for k, v in value.items():
                out[str(k)] = HLTBClient._json_safe(v)
            return out
        return str(value)

    @staticmethod
    def _result_to_raw(result: Any) -> dict[str, Any]:
        """
        Convert a howlongtobeatpy result object into a JSON-serializable dict.

        We prefer to keep the full returned payload (not just the extracted columns) so new
        derived fields can be added later without re-fetching.
        """
        raw: dict[str, Any] = {}
        try:
            attrs = dict(vars(result))
        except Exception:
            attrs = {}
        for k, v in attrs.items():
            if str(k).startswith("_"):
                continue
            raw[str(k)] = HLTBClient._json_safe(v)
        # Ensure expected keys exist even if howlongtobeatpy changes internals.
        for k in ("game_id", "game_name"):
            if k not in raw:
                raw[k] = HLTBClient._json_safe(getattr(result, k, None))
        # howlongtobeatpy can expose additional fields as properties rather than instance vars;
        # explicitly capture the high-value ones (and the raw JSON payload when present).
        for k in (
            "json_content",
            "review_score",
            "release_world",
            "profile_platforms",
            "game_web_link",
            "game_alias",
            "game_image_url",
            "all_styles",
            "coop_time",
            "mp_time",
            "main_extra",
            "completionist",
        ):
            if k in raw:
                continue
            try:
                v = getattr(result, k, None)
            except Exception:
                v = None
            if v is not None:
                raw[k] = HLTBClient._json_safe(v)
        return raw

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return

        by_id = raw.get("by_id")
        by_query = raw.get("by_query")
        if not isinstance(by_id, dict):
            logging.warning(
                "HLTB cache file is in an incompatible format; ignoring it (delete it to rebuild)."
            )
            return

        # by_id must contain raw HLTB-like payloads keyed by id (or synthetic key).
        # Allow storing `None` for negative by-id caches.
        self._by_id = {str(k): v for k, v in by_id.items() if isinstance(v, dict) or v is None}

        if isinstance(by_query, dict):
            out: dict[str, list[dict[str, Any]]] = {}
            for k, v in by_query.items():
                if not isinstance(v, list):
                    continue
                candidates: list[dict[str, Any]] = []
                for it in v:
                    if not isinstance(it, dict):
                        continue
                    candidates.append(
                        {
                            "game_id": it.get("game_id", None),
                            "game_name": it.get("game_name", "") or "",
                        }
                    )
                out[str(k)] = candidates
            self._by_query = out

    def _save_cache(self) -> None:
        self._cache_io.save_json(
            {
                "by_id": self._by_id,
                "by_query": self._by_query,
            },
            self.cache_path,
        )

    def get_by_id(self, hltb_id: int | str) -> dict[str, Any] | None:
        """
        Fetch HLTB data by ID (preferring cache).

        howlongtobeatpy implements `search_from_id(id)` by:
        - fetching the game's title for that id, then
        - searching by that title and filtering the result list by id.
        """
        hltb_id_str = str(hltb_id).strip()
        if not hltb_id_str or not hltb_id_str.isdigit():
            return None

        cached = self._by_id.get(hltb_id_str)
        if isinstance(cached, dict):
            self.stats["by_id_hit"] += 1
            return self.extract_fields(cached)
        if cached is None and hltb_id_str in self._by_id:
            self.stats["by_id_negative_hit"] += 1
            return None

        try:
            entry = self.client.search_from_id(int(hltb_id_str))
        except Exception:
            logging.warning(
                f"HLTB lookup failed for id={hltb_id_str} (error during lookup); "
                "not caching as not-found."
            )
            return None

        if not entry:
            self._by_id[hltb_id_str] = None
            self._save_cache()
            self.stats["by_id_negative_fetch"] += 1
            return None

        raw = self._result_to_raw(entry)
        raw["game_id"] = int(hltb_id_str)
        self._by_id[hltb_id_str] = raw
        self._save_cache()
        self.stats["by_id_fetch"] += 1
        return self.extract_fields(raw)

    def _query_variants(self, game_name: str) -> list[str]:
        """
        Generate query variants for HLTB.

        HLTB search can be sensitive to extra tokens like a trailing year in parentheses.
        """
        base = str(game_name or "").strip()
        if not base:
            return []

        variants: list[str] = []

        def _add(v: str) -> None:
            s = str(v or "").strip()
            if not s or s in variants:
                return
            variants.append(s)

        _add(base)

        # If the title contains a 4-digit year, try stripping a trailing "(YYYY)" or " YYYY".
        year = extract_year_hint(base)
        if year is not None:
            stripped = re.sub(r"\s*\(\s*(19\d{2}|20\d{2})\s*\)\s*$", "", base).strip()
            _add(stripped)
            stripped2 = re.sub(r"\s+(19\d{2}|20\d{2})\s*$", "", base).strip()
            _add(stripped2)

        # Normalize unicode dashes to a plain hyphen.
        dash_norm = base.replace("–", "-").replace("—", "-").replace("−", "-")
        _add(dash_norm)

        # Strip trailing punctuation that often differs between catalogs and HLTB.
        _add(re.sub(r"[!?\.]+$", "", base).strip())

        # Simplify common suffixes that can hurt HLTB recall.
        # Example observed: "Galaxy on Fire 2 Full HD" matches as "Galaxy on Fire 2 HD".
        _add(re.sub(r"\bFull\s+HD\b", "HD", base, flags=re.IGNORECASE).strip())

        # Subtitle fallbacks: try the base title before separators.
        for sep in (":", " - ", " – ", " — "):
            if sep in base:
                _add(base.split(sep, 1)[0].strip())

        # Roman numeral normalization helps cases like "Unreal Tournament III" -> "... 3".
        roman_token_map = {
            "I": "1",
            "II": "2",
            "III": "3",
            "IV": "4",
            "V": "5",
            "VI": "6",
            "VII": "7",
            "VIII": "8",
            "IX": "9",
            "X": "10",
        }

        def _roman_repl(m: re.Match[str]) -> str:
            return roman_token_map.get(m.group(0).upper(), m.group(0))

        roman_preserve = re.sub(r"\b(I|II|III|IV|V|VI|VII|VIII|IX|X)\b", _roman_repl, base)
        _add(roman_preserve)

        return variants

    def search(
        self,
        game_name: str,
        *,
        query: str | None = None,
        hltb_id: str | int | None = None,
    ) -> dict[str, Any] | None:
        # If an HLTB_ID is pinned, use it. This avoids ambiguity and doesn't depend on fuzzy
        # matching.
        if hltb_id is not None and str(hltb_id).strip() and str(hltb_id).strip() != "0":
            data = self.get_by_id(str(hltb_id).strip())
            if data:
                return data
            logging.warning(
                f"HLTB pinned id did not resolve for '{game_name}': {hltb_id}. "
                "Falling back to search."
            )

        try:
            best_score = -1
            best_query: str | None = None
            best_candidate: dict[str, Any] | None = None

            attempted: set[str] = set()

            def _try_query(q: str) -> None:
                nonlocal best_score, best_query, best_candidate

                q = str(q or "").strip()
                if not q or q in attempted:
                    return
                attempted.add(q)
                qkey = f"q:{q}"
                cached = self._by_query.get(qkey)
                if cached is not None:
                    self.stats["by_query_hit"] += 1
                    if not cached:
                        self.stats["by_query_negative_hit"] += 1
                    candidates = cached
                else:
                    raw_results = self.client.search(q) or []
                    candidates = []
                    for r in raw_results:
                        gid = getattr(r, "game_id", None)
                        name = getattr(r, "game_name", "") or ""
                        if gid is not None:
                            self._by_id[str(gid)] = self._result_to_raw(r)
                        candidates.append({"game_id": gid, "game_name": name})
                    # Cache empty results too (negative cache) by query.
                    self._by_query[qkey] = candidates
                    self._save_cache()
                    self.stats["by_query_fetch"] += 1
                    if not candidates:
                        self.stats["by_query_negative_fetch"] += 1

                if not candidates:
                    return

                # Choose the best match by similarity against the original title; the query can be
                # a heuristic variant and should not affect the score.
                scored = [
                    (fuzzy_score(game_name, str(r.get("game_name", "") or "")), r)
                    for r in candidates
                ]
                scored.sort(key=lambda x: x[0], reverse=True)
                score, candidate = scored[0]
                if score > best_score:
                    best_score = score
                    best_query = q
                    best_candidate = candidate

                # Exact match: stop early to avoid extra calls.
                if best_score >= 100:
                    return
                # High-confidence match: don't spend extra queries trying to reach 100%.
                if best_score >= HLTB.early_stop_score:
                    return

            base_query = str(query or game_name).strip()
            for q in self._query_variants(base_query):
                _try_query(q)
                if best_score >= HLTB.early_stop_score:
                    break

            # Last-resort case variants: keep match quality for a small number of stylized titles
            # that appear to be case-sensitive in HLTB search, without paying the cost for every
            # row.
            if best_candidate is None:
                _try_query(base_query.lower())
                _try_query(base_query.upper())

            if best_candidate is None:
                logging.warning(f"Not found in HLTB: '{game_name}'. No results from API.")
                return None

            # Avoid pinning clearly wrong matches; let the user override the HLTB_Query instead.
            candidate_name = str(best_candidate.get("game_name", "") or "")
            if best_score < 65:
                logging.warning(
                    f"Close match for '{game_name}': Best candidate '{candidate_name}' "
                    f"(score: {best_score}%, query={best_query!r}); not pinning."
                )
                return None

            if best_score < 100:
                logging.warning(
                    f"Close match for '{game_name}': Selected '{candidate_name}' "
                    f"(score: {best_score}%, query={best_query!r})"
                )

            best_id = (
                best_candidate.get("game_id", None) if isinstance(best_candidate, dict) else None
            )
            best_id_str = str(best_id) if best_id is not None else ""
            if best_id_str:
                cached = self._by_id.get(best_id_str)
                if isinstance(cached, dict):
                    return self.extract_fields(cached)
                logging.warning(
                    f"HLTB cache missing by_id payload for '{game_name}': id={best_id_str}. "
                    "Delete cache to rebuild."
                )
                return None

            # No stable id in the candidate list (unexpected for HLTB); do not cache synthetic
            # entries.
            logging.warning(
                f"HLTB candidate missing a stable id for '{game_name}': "
                f"name={best_candidate.get('game_name', '')!r}. Not caching."
            )
            return None

        except Exception:
            logging.warning(
                f"HLTB search request failed for '{game_name}' (error during lookup); "
                "not caching as not-found."
            )
            return None

    @staticmethod
    def extract_fields(raw: dict[str, Any]) -> dict[str, str]:
        if not isinstance(raw, dict):
            return {}
        gid = raw.get("game_id", None)
        release_world = raw.get("release_world", None)
        release_year = str(release_world) if isinstance(release_world, int) else ""
        platforms = raw.get("profile_platforms", None)
        if isinstance(platforms, list):
            platform_str = ", ".join(str(x) for x in platforms if str(x).strip())
        else:
            platform_str = ""
        # Keep provider-only fields in cache, but only emit cross-checkable metadata + the
        # core HLTB time fields into CSV outputs.
        score_100 = ""
        review_score = raw.get("review_score", None)
        if isinstance(review_score, (int, float)) and 0 <= float(review_score) <= 100:
            score_100 = str(int(round(float(review_score))))
        return {
            "HLTB_ID": str(gid) if gid is not None else "",
            "HLTB_Name": str(raw.get("game_name", "") or ""),
            "HLTB_Main": str(raw.get("main_story", "") or ""),
            "HLTB_Extra": str(raw.get("main_extra", "") or ""),
            "HLTB_Completionist": str(raw.get("completionist", "") or ""),
            "HLTB_ReleaseYear": release_year,
            "HLTB_Platforms": platform_str,
            "Score_HLTB_100": score_100,
        }

    def format_cache_stats(self) -> str:
        s = self.stats
        return (
            f"by_query hit={s['by_query_hit']} fetch={s['by_query_fetch']} "
            f"(neg hit={s['by_query_negative_hit']} fetch={s['by_query_negative_fetch']}), "
            f"by_id hit={s['by_id_hit']} fetch={s['by_id_fetch']} "
            f"(neg hit={s['by_id_negative_hit']} fetch={s['by_id_negative_fetch']}), "
            f"cache load_ms={int(s.get('cache_load_ms', 0) or 0)} "
            f"saves={int(s.get('cache_save_count', 0) or 0)} "
            f"save_ms={int(s.get('cache_save_ms', 0) or 0)}"
        )
