from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

from howlongtobeatpy import HowLongToBeat

from ..utils.utilities import (
    extract_year_hint,
    fuzzy_score,
    load_json_cache,
    normalize_game_name,
    save_json_cache,
)


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
        }
        self._by_id: dict[str, Any] = {}
        # Cache query -> lightweight candidates (id/name). Raw game payloads are cached by id.
        self._by_query: dict[str, list[dict[str, Any]]] = {}
        self._load_cache(load_json_cache(self.cache_path))
        self.client = HowLongToBeat()

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
        self._by_id = {str(k): v for k, v in by_id.items() if isinstance(v, dict)}

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
        save_json_cache(
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

        try:
            entry = self.client.search_from_id(int(hltb_id_str))
        except Exception:
            logging.warning(
                f"HLTB lookup failed for id={hltb_id_str} (error during lookup); "
                "not caching as not-found."
            )
            return None

        if not entry:
            return None

        raw = {
            "game_id": int(hltb_id_str),
            "game_name": str(getattr(entry, "game_name", "") or ""),
            "main_story": getattr(entry, "main_story", "") or "",
            "main_extra": getattr(entry, "main_extra", "") or "",
            "completionist": getattr(entry, "completionist", "") or "",
        }
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

        variants = [base]

        # If the title contains a 4-digit year, try stripping a trailing "(YYYY)" or " YYYY".
        year = extract_year_hint(base)
        if year is not None:
            stripped = re.sub(r"\s*\(\s*(19\d{2}|20\d{2})\s*\)\s*$", "", base).strip()
            if stripped and stripped not in variants:
                variants.append(stripped)
            stripped2 = re.sub(r"\s+(19\d{2}|20\d{2})\s*$", "", base).strip()
            if stripped2 and stripped2 not in variants:
                variants.append(stripped2)

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
            candidates: list[dict[str, Any]] = []
            used_query = query or game_name
            for q in self._query_variants(used_query):
                used_query = q
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
                            self._by_id[str(gid)] = {
                                "game_id": gid,
                                "game_name": name,
                                "main_story": getattr(r, "main_story", "") or "",
                                "main_extra": getattr(r, "main_extra", "") or "",
                                "completionist": getattr(r, "completionist", "") or "",
                            }
                        candidates.append({"game_id": gid, "game_name": name})
                    # Cache empty results too (negative cache) by query.
                    self._by_query[qkey] = candidates
                    self._save_cache()
                    self.stats["by_query_fetch"] += 1
                    if not candidates:
                        self.stats["by_query_negative_fetch"] += 1
                if candidates:
                    break
            if not candidates:
                logging.warning(f"Not found in HLTB: '{game_name}'. No results from API.")
                return None

            # Choose the best match by similarity
            score_target = used_query
            scored = [
                (fuzzy_score(score_target, str(r.get("game_name", "") or "")), r)
                for r in candidates
            ]
            scored.sort(key=lambda x: x[0], reverse=True)
            best_score, best = scored[0]
            if best_score < 100:
                logging.warning(
                    f"Close match for '{game_name}': Selected '{best.get('game_name', '')}' "
                    f"(score: {best_score}%)"
                )

            best_id = best.get("game_id", None) if isinstance(best, dict) else None
            best_id_str = str(best_id) if best_id is not None else ""
            if best_id_str:
                cached = self._by_id.get(best_id_str)
                if isinstance(cached, dict):
                    return self.extract_fields(cached)
                # Partial migration: fetch by id to populate by_id.
                return self.get_by_id(best_id_str)

            # No stable id: store a minimal raw payload under a synthetic key.
            name_id = f"name:{normalize_game_name(game_name)}"
            cached = self._by_id.get(name_id)
            if isinstance(cached, dict):
                return self.extract_fields(cached)
            raw = {
                "game_id": None,
                "game_name": str(best.get("game_name", "") or ""),
                "main_story": "",
                "main_extra": "",
                "completionist": "",
            }
            self._by_id[name_id] = raw
            self._save_cache()
            return self.extract_fields(raw)

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
        return {
            "HLTB_ID": str(gid) if gid is not None else "",
            "HLTB_Name": str(raw.get("game_name", "") or ""),
            "HLTB_Main": str(raw.get("main_story", "") or ""),
            "HLTB_Extra": str(raw.get("main_extra", "") or ""),
            "HLTB_Completionist": str(raw.get("completionist", "") or ""),
        }

    def format_cache_stats(self) -> str:
        s = self.stats
        return (
            f"by_query hit={s['by_query_hit']} fetch={s['by_query_fetch']} "
            f"(neg hit={s['by_query_negative_hit']} fetch={s['by_query_negative_fetch']}), "
            f"by_id hit={s['by_id_hit']} fetch={s['by_id_fetch']}"
        )
