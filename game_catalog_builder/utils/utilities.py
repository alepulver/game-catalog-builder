from __future__ import annotations

import json
import logging
import random
import re
import time
import uuid
import atexit
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import yaml
from rapidfuzz import fuzz

from ..config import CACHE, RETRY

IDENTITY_NOT_FOUND = "__NOT_FOUND__"

# ----------------------------
# Year parsing
# ----------------------------


_YEAR_HINT_RE = re.compile(r"(?:^|[\s(])(?P<year>19\d{2}|20\d{2})(?:$|[\s)])")


def extract_year_hint(text: str) -> int | None:
    """
    Extract a 4-digit year hint (1900-2100) from a string, if present.

    Intended for disambiguating provider search results, not for strict validation.
    """
    s = str(text or "").strip()
    if not s:
        return None
    m = _YEAR_HINT_RE.search(s)
    if not m:
        return None
    try:
        year = int(m.group("year"))
    except ValueError:
        return None
    if 1900 <= year <= 2100:
        return year
    return None


# ----------------------------
# Paths / Folder structure
# ----------------------------


@dataclass(frozen=True)
class ProjectPaths:
    root: Path
    data_input: Path
    data_cache: Path
    data_output: Path
    data_logs: Path
    data_experiments: Path
    data_experiments_logs: Path

    @staticmethod
    def from_root(root: str | Path) -> ProjectPaths:
        rootp = Path(root).resolve()
        return ProjectPaths(
            root=rootp,
            data_input=rootp / "data" / "input",
            data_cache=rootp / "data" / "cache",
            data_output=rootp / "data" / "output",
            data_logs=rootp / "data" / "logs",
            data_experiments=rootp / "data" / "experiments",
            data_experiments_logs=rootp / "data" / "experiments" / "logs",
        )

    def ensure(self) -> None:
        self.data_input.mkdir(parents=True, exist_ok=True)
        self.data_cache.mkdir(parents=True, exist_ok=True)
        self.data_output.mkdir(parents=True, exist_ok=True)
        self.data_logs.mkdir(parents=True, exist_ok=True)
        self.data_experiments.mkdir(parents=True, exist_ok=True)
        self.data_experiments_logs.mkdir(parents=True, exist_ok=True)


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


def load_identity_overrides(path: str | Path) -> dict[str, dict[str, str]]:
    """
    Load per-row provider IDs (and HLTB query overrides) from a CSV.

    Returns:
        Mapping:
            {RowId: {"RAWG_ID": "...", "IGDB_ID": "...", "Steam_AppID": "...",
                     "HLTB_ID": "...", "HLTB_Query": "...",
                     "Wikidata_QID": "..."}}

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
    hltb_id = col("HLTB_ID") if "HLTB_ID" in df.columns else pd.Series([""] * len(df))
    hltb_query = col("HLTB_Query") if "HLTB_Query" in df.columns else pd.Series([""] * len(df))
    wikidata = (
        col("Wikidata_QID") if "Wikidata_QID" in df.columns else pd.Series([""] * len(df))
    )

    out: dict[str, dict[str, str]] = {}
    for rid, rawg_id, igdb_id, steam_id, hltb_id_val, hltb_q, qid in zip(
        rowids.tolist(),
        rawg.tolist(),
        igdb.tolist(),
        steam.tolist(),
        hltb_id.astype(str).str.strip().tolist(),
        hltb_query.astype(str).str.strip().tolist(),
        wikidata.astype(str).str.strip().tolist(),
    ):
        if not rid:
            continue
        out[rid] = {
            "RAWG_ID": rawg_id,
            "IGDB_ID": igdb_id,
            "Steam_AppID": steam_id,
            "HLTB_ID": hltb_id_val,
            "HLTB_Query": hltb_q,
            "Wikidata_QID": qid,
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


_EDITION_TOKENS = {
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
}

_DLC_LIKE_TOKENS = {
    "soundtrack",
    "demo",
    "beta",
    "expansion",
    "pack",
    "season",
    "pass",
}


def _is_year_token(t: str) -> bool:
    return t.isdigit() and len(t) == 4 and 1900 <= int(t) <= 2100


def _token_set(s: str) -> set[str]:
    return set(normalize_game_name(s).split())


def _series_numbers_tokens(tokens: set[str]) -> set[int]:
    out: set[int] = set()
    for t in tokens:
        if not t.isdigit():
            continue
        # Avoid leading-zero “brand” tokens like 007.
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
    tokens = _token_set(name)
    return any(t in tokens for t in _DLC_LIKE_TOKENS)


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
    # difference is either a 4-digit year token or a small set of “edition” tokens (e.g. "Doom"
    # vs "Doom 2016", "Assassin's Creed" vs "Assassin's Creed Director's Cut"). This avoids
    # substring-based false positives like "Doom 3" vs "Doom 2016" and "60 Seconds!" vs
    # "60 Seconds Santa Run".
    extra_a = tokens_a - tokens_b
    extra_b = tokens_b - tokens_a
    year_only_a = bool(extra_a) and all(_is_year_token(t) for t in extra_a)
    year_only_b = bool(extra_b) and all(_is_year_token(t) for t in extra_b)

    edition_only_a = bool(extra_a) and all(t in _EDITION_TOKENS for t in extra_a)
    edition_only_b = bool(extra_b) and all(t in _EDITION_TOKENS for t in extra_b)

    allow_partial = (
        (year_only_a and not extra_b)
        or (year_only_b and not extra_a)
        or (edition_only_a and not extra_b)
        or (edition_only_b and not extra_a)
    )

    if not allow_partial:
        return int(score_sort)
    return int(max(score_sort, score_partial))


def pick_best_match(
    query: str,
    candidates: list[dict[str, Any]],
    name_key: str = "name",
    *,
    year_hint: int | None = None,
    year_getter: Callable[[dict[str, Any]], int | None] | None = None,
) -> tuple[dict[str, Any] | None, int, list[tuple[str, int]]]:
    """
    Given a query and a list of dicts (candidates), choose the candidate with the best fuzzy score.
    Returns (best_candidate, best_score, top_matches).
    top_matches is a list of (name, score) tuples for the top 5 matches (excluding the best itself).
    """
    scored = []
    q_tokens = _token_set(query)
    q_series = _series_numbers_tokens(q_tokens)
    q_norm = normalize_game_name(query)
    q_has_non_year_number = any(t.isdigit() and not _is_year_token(t) for t in q_tokens)
    for c in candidates:
        cname = str(c.get(name_key, "") or "")
        score = fuzzy_score(query, cname)

        c_tokens = _token_set(cname)
        c_series = _series_numbers_tokens(c_tokens)
        c_norm = normalize_game_name(cname)

        # Penalize likely sequel matches when the query has no sequel number.
        series_penalty = 15 if (not q_series and c_series) else 0
        # Penalize different series numbers when both sides have them (e.g. "Postal 4" should not
        # match "Postal 2").
        series_penalty += 20 if (q_series and c_series and q_series.isdisjoint(c_series)) else 0
        dlc_penalty = 20 if _looks_dlc_like(cname) else 0

        diff = q_tokens.symmetric_difference(c_tokens)
        year_diff = sum(1 for t in diff if _is_year_token(t))
        non_year_diff = len(diff) - year_diff

        adjusted = max(0, score - series_penalty - dlc_penalty)

        # If the query includes a non-year number, treat prefix matches as higher confidence.
        # Example: "Postal 4" should match "POSTAL 4: No Regerts" even though the subtitle
        # would otherwise lower token_sort_ratio.
        if q_has_non_year_number and q_norm and c_norm.startswith(q_norm):
            adjusted += 25

        year_delta: int | None = None
        if year_hint is not None and year_getter is not None:
            try:
                cand_year = year_getter(c)
            except Exception:
                cand_year = None
            if cand_year is None:
                # If the user provided a year hint, prefer candidates that provide a year at all.
                # This avoids picking upcoming/placeholder entries with missing release dates.
                adjusted -= 8
            else:
                year_delta = abs(int(cand_year) - int(year_hint))
                if year_delta == 0:
                    adjusted += 10
                elif year_delta <= 1:
                    adjusted += 6
                elif year_delta <= 2:
                    adjusted += 3
                elif year_delta >= 15:
                    adjusted -= 14
                elif year_delta >= 10:
                    adjusted -= 10
                elif year_delta >= 5:
                    adjusted -= 6
        adjusted = max(0, min(100, adjusted))

        # If we have an exact token match (raw score 100 and no non-year token differences),
        # prefer it over year-based adjustments. This avoids surprising selections like choosing
        # an edition/sequel when the base title exists as an exact match.
        exact_token_match = (
            score >= 100
            and non_year_diff == 0
            and year_diff == 0
            and series_penalty == 0
            and dlc_penalty == 0
        )
        if exact_token_match:
            adjusted = 100

        raw_id = c.get("id")
        try:
            id_num = int(str(raw_id)) if raw_id is not None else 10**18
        except ValueError:
            id_num = 10**18

        scored.append(
            (
                c,
                cname,
                score,
                adjusted,
                exact_token_match,
                dlc_penalty > 0,
                non_year_diff,
                year_diff,
                id_num,
                year_delta,
            )
        )

    # Prefer closer year match when provided.
    scored.sort(
        key=lambda x: (
            -x[3],  # adjusted score
            -x[2],  # raw score
            -int(x[4]),  # exact token match preferred
            (x[8] if x[8] is not None else 10**9),  # year delta (lower is better)
            x[5],  # DLC-like (False preferred)
            x[6],  # non-year token diff (lower is better)
            x[7],  # year token diff (lower is better)
            len(x[1]),  # shorter title
            x[8],  # smaller numeric id
        )
    )

    if not scored:
        return None, -1, []

    best, best_name, best_score, best_adjusted, *_ = scored[0]

    # Get top 5 matches (excluding the best itself)
    top_matches = [
        (name, score)
        for _, name, score, *_ in scored[1:6]  # Top 5 after the best
        if score > 0
    ]

    return best, int(best_adjusted), top_matches


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
    retries: int = RETRY.retries,
    base_sleep_s: float = RETRY.base_sleep_s,
    jitter_s: float = RETRY.jitter_s,
    retry_on: tuple[type, ...] = (Exception,),
    on_fail_return: Any = None,
    context: str | None = None,
    retry_stats: dict[str, Any] | None = None,
) -> Any:
    """
    Execute fn with retries and exponential backoff.
    """
    last_exc: BaseException | None = None
    for attempt in range(retries):
        try:
            return fn()
        except retry_on as e:
            last_exc = e
            retry_after_s: float | None = None
            status: int | None = None
            is_429 = False
            is_network = False
            is_http = False
            try:
                import requests  # local import to avoid hard dependency at import time

                if isinstance(last_exc, requests.exceptions.HTTPError):
                    is_http = True
                    resp = getattr(last_exc, "response", None)
                    status = getattr(resp, "status_code", None)
                    if status == 429:
                        is_429 = True
                        headers = getattr(resp, "headers", {}) or {}
                        try:
                            ra = str(headers.get("Retry-After", "") or "").strip()
                            if ra:
                                retry_after_s = float(ra)
                        except Exception:
                            retry_after_s = None
                        if retry_after_s is None:
                            retry_after_s = RETRY.http_429_default_retry_after_s
                else:
                    net_types = (
                        requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout,
                        requests.exceptions.SSLError,
                    )
                    if isinstance(last_exc, net_types):
                        is_network = True
            except Exception:
                retry_after_s = None
                status = None
                is_429 = False
                is_network = False
                is_http = False

            if retry_stats is not None:
                if is_429:
                    retry_stats["http_429"] = int(retry_stats.get("http_429", 0)) + 1
                if is_network:
                    retry_stats["network_errors"] = int(retry_stats.get("network_errors", 0)) + 1
                if is_http:
                    retry_stats["http_errors"] = int(retry_stats.get("http_errors", 0)) + 1

            if attempt == retries - 1:
                if context and last_exc is not None:
                    # Make network-offline situations obvious in logs, and distinct from
                    # provider "not found" cases.
                    try:
                        import requests  # local import to avoid hard dependency at import time

                        net_types = (
                            requests.exceptions.ConnectionError,
                            requests.exceptions.Timeout,
                            requests.exceptions.SSLError,
                        )
                        http_types = (requests.exceptions.HTTPError,)
                        if isinstance(last_exc, net_types):
                            logging.error(
                                f"[NETWORK] {context}: {type(last_exc).__name__}: {last_exc}"
                            )
                        elif isinstance(last_exc, http_types):
                            logging.error(
                                f"[HTTP] {context}: {type(last_exc).__name__}: {last_exc}"
                            )
                        else:
                            logging.error(
                                f"[REQUEST] {context}: {type(last_exc).__name__}: {last_exc}"
                            )
                    except Exception:
                        logging.error(f"[REQUEST] {context}: {type(last_exc).__name__}: {last_exc}")
                if retry_stats is not None:
                    if is_network:
                        retry_stats["network_failures"] = int(
                            retry_stats.get("network_failures", 0)
                        ) + 1
                    if is_http:
                        retry_stats["http_failures"] = int(retry_stats.get("http_failures", 0)) + 1
                return on_fail_return
            sleep = base_sleep_s * (2**attempt) + random.uniform(0, jitter_s)
            if retry_after_s is not None and retry_after_s > 0:
                sleep = max(sleep, retry_after_s)
            if retry_stats is not None:
                retry_stats["retry_attempts"] = int(retry_stats.get("retry_attempts", 0)) + 1
                if is_429:
                    retry_stats["http_429_retries"] = int(retry_stats.get("http_429_retries", 0)) + 1
                    retry_stats["http_429_backoff_ms"] = int(
                        retry_stats.get("http_429_backoff_ms", 0)
                    ) + int(round(sleep * 1000.0))
            time.sleep(sleep)
    return on_fail_return


def network_failures_count(stats: dict[str, Any] | None) -> int:
    if not stats:
        return 0
    try:
        return int(stats.get("network_failures", 0) or 0)
    except Exception:
        return 0


def raise_on_new_network_failure(
    stats: dict[str, Any] | None, *, before: int, context: str
) -> None:
    """
    Raise a clear error when a network failure happened during a provider request.

    Use this when a cache miss required a real HTTP call: we prefer failing fast rather than
    producing partial results that look like "not found".
    """
    after = network_failures_count(stats)
    if after > before:
        raise RuntimeError(
            f"Network unavailable while calling {context}. Enable internet access and rerun."
        )


@dataclass
class CacheIOTracker:
    """
    Track JSON cache load/save counts and time in milliseconds.

    Clients should use this instead of duplicating perf_counter timing logic.
    """

    stats: dict[str, Any]
    prefix: str = "cache"
    min_interval_s: float | None = None

    def __post_init__(self) -> None:
        self.stats.setdefault(f"{self.prefix}_load_count", 0)
        self.stats.setdefault(f"{self.prefix}_load_ms", 0)
        self.stats.setdefault(f"{self.prefix}_save_count", 0)
        self.stats.setdefault(f"{self.prefix}_save_ms", 0)
        self._last_save_s = 0.0
        self._pending: tuple[dict[str, Any], Path] | None = None
        atexit.register(self.flush)

    def load_json(self, path: str | Path) -> dict[str, Any]:
        t0 = time.perf_counter()
        raw = load_json_cache(path)
        t1 = time.perf_counter()
        self.stats[f"{self.prefix}_load_count"] = int(
            self.stats.get(f"{self.prefix}_load_count", 0) or 0
        ) + 1
        self.stats[f"{self.prefix}_load_ms"] = int(self.stats.get(f"{self.prefix}_load_ms", 0) or 0) + int(
            round((t1 - t0) * 1000.0)
        )
        return raw

    def save_json(self, cache: dict[str, Any], path: str | Path) -> None:
        # Throttle full-cache rewrites; caches can be large and write-heavy runs suffer otherwise.
        p = Path(path)
        now = time.monotonic()
        if self.min_interval_s is not None:
            min_interval = float(self.min_interval_s)
        else:
            min_interval = float(getattr(CACHE, "save_min_interval_small_s", 0.0) or 0.0)
        if min_interval > 0 and (now - self._last_save_s) < min_interval:
            self._pending = (cache, p)
            return

        self._save_now(cache, p)

    def flush(self) -> None:
        pending = self._pending
        if pending is None:
            return
        cache, path = pending
        self._pending = None
        self._save_now(cache, path)

    def _save_now(self, cache: dict[str, Any], path: Path) -> None:
        t0 = time.perf_counter()
        save_json_cache(cache, path)
        t1 = time.perf_counter()
        self._last_save_s = time.monotonic()
        dur_ms = int(round((t1 - t0) * 1000.0))
        self.stats[f"{self.prefix}_save_count"] = int(
            self.stats.get(f"{self.prefix}_save_count", 0) or 0
        ) + 1
        self.stats[f"{self.prefix}_save_ms"] = int(self.stats.get(f"{self.prefix}_save_ms", 0) or 0) + dur_ms

        slow_ms = int(getattr(CACHE, "slow_save_log_ms", 0) or 0)
        if slow_ms > 0 and dur_ms >= slow_ms:
            logging.info(f"[CACHE] Wrote '{path.name}' in {dur_ms}ms")

    @staticmethod
    def format_io(stats: dict[str, Any] | None, *, prefix: str = "cache") -> str:
        if not stats:
            return "cache load_ms=0 saves=0 save_ms=0"
        load_ms = int(stats.get(f"{prefix}_load_ms", 0) or 0)
        save_count = int(stats.get(f"{prefix}_save_count", 0) or 0)
        save_ms = int(stats.get(f"{prefix}_save_ms", 0) or 0)
        return (
            f"{prefix} load_ms={load_ms} "
            f"saves={save_count} "
            f"save_ms={save_ms}"
        )


def iter_chunks(items: list[Any], chunk_size: int) -> list[list[Any]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if not items:
        return []
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


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


from ..schema import PUBLIC_DEFAULT_COLS
