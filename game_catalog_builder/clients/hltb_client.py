from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from howlongtobeatpy import HowLongToBeat

from ..utils.utilities import (
    fuzzy_score,
    load_json_cache,
    normalize_game_name,
    save_json_cache,
)


class HLTBClient:
    def __init__(self, cache_path: str | Path):
        self.cache_path = Path(cache_path)
        self._by_id: dict[str, Any] = {}
        self._by_name: dict[str, str | None] = {}
        self._load_cache(load_json_cache(self.cache_path))
        self.client = HowLongToBeat()

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return

        by_id = raw.get("by_id")
        by_name = raw.get("by_name")
        if isinstance(by_id, dict) and isinstance(by_name, dict):
            self._by_id = {str(k): v for k, v in by_id.items()}
            self._by_name = {str(k): (str(v) if v else None) for k, v in by_name.items()}

    def _save_cache(self) -> None:
        save_json_cache({"by_id": self._by_id, "by_name": self._by_name}, self.cache_path)

    def search(self, game_name: str) -> dict[str, Any] | None:
        key = normalize_game_name(game_name)
        if key in self._by_name:
            hltb_id = self._by_name[key]
            if not hltb_id:
                return None
            return self._by_id.get(str(hltb_id))

        try:
            results = self.client.search(game_name)
            if not results:
                logging.warning(f"Not found in HLTB: '{game_name}'. No results from API.")
                self._by_name[key] = None
                self._save_cache()
                return None

            # Choose the best match by similarity
            scored = [(fuzzy_score(game_name, r.game_name), r) for r in results]
            scored.sort(key=lambda x: x[0], reverse=True)
            best_score, best = scored[0]
            if best_score < 100:
                logging.warning(
                    f"Close match for '{game_name}': Selected '{best.game_name}' "
                    f"(score: {best_score}%)"
                )

            best_id = getattr(best, "game_id", None)
            data = {
                "HLTB_Name": str(getattr(best, "game_name", "") or ""),
                "HLTB_Main": best.main_story or "",
                "HLTB_Extra": best.main_extra or "",
                "HLTB_Completionist": best.completionist or "",
            }

            if best_id is not None:
                best_id_str = str(best_id)
                self._by_id[best_id_str] = data
                self._by_name[key] = best_id_str
            else:
                # Fallback: cache by normalized name when no stable id is available.
                name_id = f"name:{key}"
                self._by_id[name_id] = data
                self._by_name[key] = name_id
            self._save_cache()
            return data

        except Exception:
            logging.warning(f"Not found in HLTB: '{game_name}'. Error during lookup.")
            self._by_name[key] = None
            self._save_cache()
            return None
