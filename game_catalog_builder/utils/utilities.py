from __future__ import annotations

import json
import os
import re
import time
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import pandas as pd
import yaml
from rapidfuzz import fuzz


# ----------------------------
# Paths / Folder structure
# ----------------------------

@dataclass(frozen=True)
class ProjectPaths:
    root: Path
    data_input: Path
    data_raw: Path
    data_processed: Path

    @staticmethod
    def from_root(root: str | Path) -> "ProjectPaths":
        rootp = Path(root).resolve()
        return ProjectPaths(
            root=rootp,
            data_input=rootp / "data" / "input",
            data_raw=rootp / "data" / "raw",
            data_processed=rootp / "data" / "processed",
        )

    def ensure(self) -> None:
        self.data_input.mkdir(parents=True, exist_ok=True)
        self.data_raw.mkdir(parents=True, exist_ok=True)
        self.data_processed.mkdir(parents=True, exist_ok=True)


# ----------------------------
# CSV Helpers
# ----------------------------

def read_csv(path: str | Path) -> pd.DataFrame:
    """Read CSV preserving strings and avoiding problematic type inference."""
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def write_csv(df: pd.DataFrame, path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def ensure_columns(df: pd.DataFrame, cols_with_defaults: Dict[str, Any]) -> pd.DataFrame:
    """Create columns if they don't exist, with a default value."""
    for col, default in cols_with_defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


# ----------------------------
# Name normalization
# ----------------------------

_ROMAN_MAP = {
    " i ": " 1 ",
    " ii ": " 2 ",
    " iii ": " 3 ",
    " iv ": " 4 ",
    " v ": " 5 ",
    " vi ": " 6 ",
    " vii ": " 7 ",
    " viii ": " 8 ",
    " ix ": " 9 ",
    " x ": " 10 ",
}


def normalize_game_name(name: str) -> str:
    """
    Normalize names to improve matching between catalogs.
    - lowercase
    - remove punctuation
    - collapse spaces
    - convert '®™' etc
    - optional: roman numerals to arabic for typical cases (I, II, III...)
    """
    s = (name or "").strip().lower()
    s = s.replace("™", "").replace("®", "").replace("©", "")
    s = re.sub(r"[\(\)\[\]\{\}]", " ", s)
    s = re.sub(r"[’'`]", "", s)  # apostrophes
    s = re.sub(r"[:\-–—_/\\|]", " ", s)
    s = re.sub(r"[.,!?+*&%$#@~]", " ", s)

    s = f" {s} "
    for k, v in _ROMAN_MAP.items():
        s = s.replace(k, v)

    s = re.sub(r"\s+", " ", s).strip()
    return s


# ----------------------------
# Fuzzy matching
# ----------------------------

def fuzzy_score(a: str, b: str) -> int:
    """Calculate fuzzy matching score between two strings."""
    return int(fuzz.partial_ratio(normalize_game_name(a), normalize_game_name(b)))


def pick_best_match(query: str, candidates: list[Dict[str, Any]], name_key: str = "name") -> Tuple[Optional[Dict[str, Any]], int, list[Tuple[str, int]]]:
    """
    Given a query and a list of dicts (candidates), choose the candidate with the best fuzzy score.
    Returns (best_candidate, best_score, top_matches).
    top_matches is a list of (name, score) tuples for the top 5 matches (excluding the best itself).
    """
    scored = []
    for c in candidates:
        cname = str(c.get(name_key, "") or "")
        score = fuzzy_score(query, cname)
        scored.append((c, cname, score))

    # Sort by score descending
    scored.sort(key=lambda x: x[2], reverse=True)

    if not scored:
        return None, -1, []

    best, best_name, best_score = scored[0]

    # Get top 5 matches (excluding the best itself)
    top_matches = [
        (name, score) for _, name, score in scored[1:6]  # Top 5 after the best
        if score > 0
    ]

    return best, best_score, top_matches


# ----------------------------
# JSON Cache (by name or id)
# ----------------------------

def load_json_cache(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_json_cache(cache: Dict[str, Any], path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


# ----------------------------
# Rate limiting + retries
# ----------------------------

class RateLimiter:
    """
    Simple rate limiter: enforces minimum interval between requests.
    """
    def __init__(self, min_interval_s: float = 1.0):
        self.min_interval_s = float(min_interval_s)
        self._last = 0.0

    def wait(self) -> None:
        now = time.time()
        delta = now - self._last
        if delta < self.min_interval_s:
            time.sleep(self.min_interval_s - delta)
        self._last = time.time()


def with_retries(
    fn: Callable[[], Any],
    *,
    retries: int = 3,
    base_sleep_s: float = 1.0,
    jitter_s: float = 0.3,
    retry_on: Tuple[type, ...] = (Exception,),
    on_fail_return: Any = None,
) -> Any:
    """
    Execute fn with retries and exponential backoff.
    """
    for attempt in range(retries):
        try:
            return fn()
        except retry_on as e:
            if attempt == retries - 1:
                return on_fail_return
            sleep = base_sleep_s * (2 ** attempt) + random.uniform(0, jitter_s)
            time.sleep(sleep)
    return on_fail_return


# ----------------------------
# "Is it already processed?"
# ----------------------------

def is_row_processed(df: pd.DataFrame, idx: int, required_cols: list[str]) -> bool:
    """
    Consider 'processed' if all required columns have non-empty values.
    """
    for col in required_cols:
        if col not in df.columns:
            return False
        val = str(df.at[idx, col] or "").strip()
        if val == "":
            return False
    return True


# ----------------------------
# Credentials loading
# ----------------------------

def load_credentials(credentials_path: str | Path | None = None) -> Dict[str, Any]:
    """
    Load credentials from a YAML file.
    
    Args:
        credentials_path: Path to credentials.yaml file. If None, looks for
                         credentials.yaml in the project root.
    
    Returns:
        Dictionary with credentials (e.g., {'igdb': {...}, 'rawg': {...}})
    """
    if credentials_path is None:
        # Try to find credentials.yaml in the project root
        # Look for it relative to the utilities.py file (go up to project root)
        root = Path(__file__).resolve().parent.parent.parent
        credentials_path = root / "credentials.yaml"
    else:
        credentials_path = Path(credentials_path)
    
    if not credentials_path.exists():
        raise FileNotFoundError(
            f"Credentials file not found: {credentials_path}\n"
            "Please create credentials.yaml with your API keys."
        )
    
    with open(credentials_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ----------------------------
# Default public columns (for merges)
# ----------------------------

PUBLIC_DEFAULT_COLS: Dict[str, Any] = {
    # RAWG
    "RAWG_ID": "",
    "RAWG_Year": "",
    "RAWG_Genre": "",
    "RAWG_Genre2": "",
    "RAWG_Platforms": "",
    "RAWG_Tags": "",
    "RAWG_Rating": "",
    "RAWG_RatingsCount": "",
    "RAWG_Metacritic": "",

    # IGDB
    "IGDB_ID": "",
    "IGDB_Genres": "",
    "IGDB_Themes": "",
    "IGDB_GameModes": "",
    "IGDB_Perspectives": "",
    "IGDB_Franchise": "",
    "IGDB_Engine": "",
    "IGDB_Companies": "",

    # Steam
    "Steam_AppID": "",
    "Steam_Tags": "",
    "Steam_ReviewCount": "",
    "Steam_ReviewPercent": "",
    "Steam_Price": "",
    "Steam_Categories": "",

    # SteamSpy
    "SteamSpy_Owners": "",
    "SteamSpy_Players": "",
    "SteamSpy_CCU": "",
    "SteamSpy_PlaytimeAvg": "",

    # HLTB
    "HLTB_Main": "",
    "HLTB_Extra": "",
    "HLTB_Completionist": "",
}
