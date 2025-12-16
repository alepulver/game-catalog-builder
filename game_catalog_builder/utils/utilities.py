from __future__ import annotations

import json
import random
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import yaml
from rapidfuzz import fuzz

IDENTITY_NOT_FOUND = "__NOT_FOUND__"

# ----------------------------
# Paths / Folder structure
# ----------------------------


@dataclass(frozen=True)
class ProjectPaths:
    root: Path
    data_input: Path
    data_cache: Path
    data_output: Path

    @staticmethod
    def from_root(root: str | Path) -> ProjectPaths:
        rootp = Path(root).resolve()
        return ProjectPaths(
            root=rootp,
            data_input=rootp / "data" / "input",
            data_cache=rootp / "data" / "cache",
            data_output=rootp / "data" / "output",
        )

    def ensure(self) -> None:
        self.data_input.mkdir(parents=True, exist_ok=True)
        self.data_cache.mkdir(parents=True, exist_ok=True)
        self.data_output.mkdir(parents=True, exist_ok=True)


# ----------------------------
# CSV Helpers
# ----------------------------


def read_csv(path: str | Path) -> pd.DataFrame:
    """Read CSV preserving strings and avoiding problematic type inference."""
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def write_csv(df: pd.DataFrame, path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def ensure_columns(df: pd.DataFrame, cols_with_defaults: dict[str, Any]) -> pd.DataFrame:
    """Create columns if they don't exist, with a default value."""
    for col, default in cols_with_defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


def ensure_row_ids(df: pd.DataFrame, *, col: str = "RowId") -> tuple[pd.DataFrame, int]:
    """
    Ensure a dataframe contains stable row identifiers.

    Returns (df, created_count).
    """
    out = df.copy()
    created = 0

    if col not in out.columns:
        out.insert(0, col, [""] * len(out))

    vals = out[col].astype(str).fillna("").str.strip()
    missing_mask = vals == ""
    if missing_mask.any():
        count = int(missing_mask.sum())
        out.loc[missing_mask, col] = [f"rid:{uuid.uuid4()}" for _ in range(count)]
        created += count

    # Ensure uniqueness (keep first occurrence, regenerate the rest).
    vals = out[col].astype(str).fillna("").str.strip()
    dup_mask = vals.duplicated(keep="first")
    if dup_mask.any():
        count = int(dup_mask.sum())
        out.loc[dup_mask, col] = [f"rid:{uuid.uuid4()}" for _ in range(count)]
        created += count

    return out, created


def ensure_row_ids_in_input(path: str | Path, *, col: str = "RowId") -> pd.DataFrame:
    """
    Backwards-compatible helper: ensure RowId exists and persist to the same CSV path.

    Prefer using ensure_row_ids(df) and writing to a new file when you don't want to modify the
    original input.
    """
    p = Path(path)
    df = read_csv(p)
    out, created = ensure_row_ids(df, col=col)
    if created > 0 or col not in df.columns:
        write_csv(out, p)
    return out


def load_identity_overrides(path: str | Path) -> dict[str, dict[str, str]]:
    """
    Load per-row provider IDs from Games_Identity.csv.

    Returns:
        {RowId: {"RAWG_ID": "...", "IGDB_ID": "...", "Steam_AppID": "...", "HLTB_Query": "..."}}

    Uses the ID columns directly. If a value is empty, the provider will fall back to searching.
    """
    p = Path(path)
    if not p.exists():
        return {}

    df = read_csv(p)
    if "RowId" not in df.columns:
        return {}

    def col(name: str) -> pd.Series:
        return df[name].astype(str).str.strip() if name in df.columns else pd.Series([""] * len(df))

    rowids = col("RowId")
    rawg = col("RAWG_ID")
    igdb = col("IGDB_ID")
    steam = col("Steam_AppID")
    hltb_query = col("HLTB_Query") if "HLTB_Query" in df.columns else pd.Series([""] * len(df))

    out: dict[str, dict[str, str]] = {}
    for rid, rawg_id, igdb_id, steam_id, hltb_q in zip(
        rowids.tolist(),
        rawg.tolist(),
        igdb.tolist(),
        steam.tolist(),
        hltb_query.astype(str).str.strip().tolist(),
    ):
        if not rid:
            continue
        out[rid] = {
            "RAWG_ID": rawg_id,
            "IGDB_ID": igdb_id,
            "Steam_AppID": steam_id,
            "HLTB_Query": hltb_q,
        }
    return out


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
    """
    Calculate fuzzy matching score between two strings.

    Uses a conservative default (token_sort_ratio) to avoid false 100% substring matches, while
    still allowing common year/edition cases (e.g. "Doom" vs "Doom 2016") via partial_ratio.
    """
    na = normalize_game_name(a)
    nb = normalize_game_name(b)
    score_sort = float(fuzz.token_sort_ratio(na, nb))
    score_partial = float(fuzz.partial_ratio(na, nb))

    tokens_a = set(na.split())
    tokens_b = set(nb.split())

    # Only allow partial matches when one side is a strict superset of the other and the only
    # difference is a 4-digit year token (e.g. "Doom" vs "Doom 2016"). This avoids substring-based
    # false positives like "Doom 3" vs "Doom 2016" and "60 Seconds!" vs "60 Seconds Santa Run".
    extra_a = tokens_a - tokens_b
    extra_b = tokens_b - tokens_a
    year_only_a = bool(extra_a) and all(t.isdigit() and len(t) == 4 for t in extra_a)
    year_only_b = bool(extra_b) and all(t.isdigit() and len(t) == 4 for t in extra_b)
    allow_partial = (year_only_a and not extra_b) or (year_only_b and not extra_a)

    if not allow_partial:
        return int(score_sort)
    return int(max(score_sort, score_partial))


def pick_best_match(
    query: str,
    candidates: list[dict[str, Any]],
    name_key: str = "name",
) -> tuple[dict[str, Any] | None, int, list[tuple[str, int]]]:
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
        (name, score)
        for _, name, score in scored[1:6]  # Top 5 after the best
        if score > 0
    ]

    return best, best_score, top_matches


# ----------------------------
# JSON Cache (by name or id)
# ----------------------------


def load_json_cache(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_json_cache(cache: dict[str, Any], path: str | Path) -> None:
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
        # Use monotonic time to avoid issues if the system clock changes.
        now = time.monotonic()
        delta = now - self._last
        if delta < self.min_interval_s:
            time.sleep(self.min_interval_s - delta)
        self._last = time.monotonic()


def with_retries(
    fn: Callable[[], Any],
    *,
    retries: int = 3,
    base_sleep_s: float = 1.0,
    jitter_s: float = 0.3,
    retry_on: tuple[type, ...] = (Exception,),
    on_fail_return: Any = None,
) -> Any:
    """
    Execute fn with retries and exponential backoff.
    """
    for attempt in range(retries):
        try:
            return fn()
        except retry_on:
            if attempt == retries - 1:
                return on_fail_return
            sleep = base_sleep_s * (2**attempt) + random.uniform(0, jitter_s)
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


def load_credentials(credentials_path: str | Path | None = None) -> dict[str, Any]:
    """
    Load credentials from a YAML file.

    Args:
        credentials_path: Path to credentials.yaml file. If None, looks for
                         data/credentials.yaml in the project root.

    Returns:
        Dictionary with credentials (e.g., {'igdb': {...}, 'rawg': {...}})
    """
    if credentials_path is None:
        # Try to find data/credentials.yaml in the project root
        # Look for it relative to the utilities.py file (go up to project root)
        root = Path(__file__).resolve().parent.parent.parent
        credentials_path = root / "data" / "credentials.yaml"
    else:
        credentials_path = Path(credentials_path)

    if not credentials_path.exists():
        raise FileNotFoundError(
            f"Credentials file not found: {credentials_path}\n"
            "Please create data/credentials.yaml with your API keys."
        )

    with open(credentials_path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ----------------------------
# Default public columns (for merges)
# ----------------------------

PUBLIC_DEFAULT_COLS: dict[str, Any] = {
    # Stable input row key
    "RowId": "",
    # RAWG
    "RAWG_ID": "",
    "RAWG_Name": "",
    "RAWG_Released": "",
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
    "IGDB_Name": "",
    "IGDB_Year": "",
    "IGDB_Platforms": "",
    "IGDB_Genres": "",
    "IGDB_Themes": "",
    "IGDB_GameModes": "",
    "IGDB_Perspectives": "",
    "IGDB_Franchise": "",
    "IGDB_Engine": "",
    "IGDB_Companies": "",
    "IGDB_SteamAppID": "",
    # Steam
    "Steam_AppID": "",
    "Steam_Name": "",
    "Steam_ReleaseYear": "",
    "Steam_Platforms": "",
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
    "HLTB_Name": "",
    "HLTB_Main": "",
    "HLTB_Extra": "",
    "HLTB_Completionist": "",
}
