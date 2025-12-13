from __future__ import annotations

from typing import Dict, Any, Optional
from pathlib import Path

from howlongtobeatpy import HowLongToBeat

from ..utils.utilities import (
    normalize_game_name,
    load_json_cache,
    save_json_cache,
    fuzzy_score,
)


class HLTBClient:
    def __init__(self, cache_path: str | Path):
        self.cache_path = Path(cache_path)
        self.cache: Dict[str, Any] = load_json_cache(self.cache_path)
        self.client = HowLongToBeat()

    def search(self, game_name: str) -> Optional[Dict[str, Any]]:
        key = normalize_game_name(game_name)
        if key in self.cache:
            return self.cache[key]

        try:
            results = self.client.search(game_name)
            if not results:
                self.cache[key] = None
                save_json_cache(self.cache, self.cache_path)
                return None

            # Choose the best match by similarity
            best = max(results, key=lambda r: fuzzy_score(game_name, r.game_name))

            data = {
                "HLTB_Main": best.main_story or "",
                "HLTB_Extra": best.main_extra or "",
                "HLTB_Completionist": best.completionist or "",
            }

            self.cache[key] = data
            save_json_cache(self.cache, self.cache_path)
            return data

        except Exception:
            self.cache[key] = None
            save_json_cache(self.cache, self.cache_path)
            return None
