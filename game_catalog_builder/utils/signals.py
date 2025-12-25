from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ..config import SIGNALS
from .company import normalize_company_name

_COMPANY_SPLIT_RE = re.compile(r"(?i)\s*(?:,|/|&|\band\b|\bwith\b|\+)\s*")


def iter_company_name_variants(value: str) -> list[str]:
    """
    Return a small set of plausible company-name variants for matching tiers:
    - normalized original string
    - if the value looks like multiple companies joined in one string, split and normalize parts
    """
    raw = str(value or "").strip()
    if not raw:
        return []
    out: list[str] = []
    n0 = normalize_company_name(raw)
    if n0:
        out.append(n0)

    # Heuristic: if it's long and contains separators, it might be multiple studios.
    if len(raw) >= 18 and _COMPANY_SPLIT_RE.search(raw):
        parts = [p.strip() for p in _COMPANY_SPLIT_RE.split(raw) if p.strip()]
        for p in parts:
            np = normalize_company_name(p)
            if np and np not in out:
                out.append(np)
    return out


def _parse_int(value: Any) -> int | None:
    s = str(value or "").strip()
    if not s:
        return None
    # handle floats serialized by CSV readers, e.g. "123.0"
    if s.endswith(".0") and s[:-2].isdigit():
        return int(s[:-2])
    try:
        f = float(s)
        if f.is_integer():
            return int(f)
    except Exception:
        pass
    # handle "1,234" or "1 234"
    s2 = re.sub(r"[,\s]", "", s)
    if s2.isdigit():
        return int(s2)
    return None


def _parse_float(value: Any) -> float | None:
    s = str(value or "").strip()
    if not s:
        return None
    if s.endswith(".0") and s[:-2].isdigit():
        return float(s[:-2])
    try:
        return float(s)
    except Exception:
        return None


_OWNERS_RANGE_RE = re.compile(
    r"^\s*(?P<low>[\d,\s]+)\s*(?:\.\.|-)\s*(?P<high>[\d,\s]+)\s*$"
)


def parse_steamspy_owners_range(owners: Any) -> tuple[int | None, int | None, int | None]:
    """
    Parse SteamSpy `owners` strings like:
      - "1,000,000 .. 2,000,000"
      - "1000000..2000000"

    Returns (low, high, mid).
    """
    s = str(owners or "").strip()
    if not s:
        return (None, None, None)

    m = _OWNERS_RANGE_RE.match(s)
    if not m:
        return (None, None, None)

    low = _parse_int(m.group("low"))
    high = _parse_int(m.group("high"))
    if low is None or high is None or low <= 0 or high <= 0:
        return (None, None, None)
    if high < low:
        low, high = high, low
    mid = int(round((low + high) / 2.0))
    return (low, high, mid)


def _log_weight(count: int | None) -> float:
    if count is None or count <= 0:
        return 0.0
    # log10(1+count) yields a nice 0..N weight range; add 1 to keep small counts relevant.
    return 1.0 + math.log10(1.0 + float(count))


def _weighted_avg(pairs: list[tuple[float, float]]) -> float | None:
    num = 0.0
    den = 0.0
    for value, weight in pairs:
        if weight <= 0:
            continue
        num += value * weight
        den += weight
    if den <= 0:
        return None
    return num / den


def _log_scale_0_100(value: int | None, *, log10_min: float, log10_max: float) -> float | None:
    if value is None or value <= 0:
        return None
    if log10_max <= log10_min:
        return None
    x = math.log10(float(value))
    t = (x - log10_min) / (log10_max - log10_min)
    if t < 0:
        t = 0.0
    if t > 1:
        t = 1.0
    return t * 100.0


def load_production_tiers(path: str | Path) -> dict[str, dict[str, str]]:
    """
    Load a production tier mapping file.

    Format:
      publishers:
        \"Publisher Name\": \"AAA\"
      developers:
        \"Developer Name\": \"Indie\"
    """
    p = Path(path)
    if not p.exists():
        return {"publishers": {}, "developers": {}}
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    pubs = data.get("publishers") if isinstance(data, dict) else {}
    devs = data.get("developers") if isinstance(data, dict) else {}
    return {
        "publishers": pubs if isinstance(pubs, dict) else {},
        "developers": devs if isinstance(devs, dict) else {},
    }


def _split_csv_list(s: Any) -> list[str]:
    raw = str(s or "").strip()
    if not raw:
        return []
    if raw.casefold() in {"nan", "none", "null"}:
        return []
    # Stored as a JSON array in a CSV cell (e.g. ["Company, Inc."]).
    if not raw.startswith("["):
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(x).strip() for x in parsed if str(x).strip()]


def compute_production_tier(
    row: dict[str, Any], mapping: dict[str, dict[str, str]]
) -> tuple[str, str]:
    """
    Compute (tier, reason) using Steam developer/publisher names.
    """
    pubs = mapping.get("publishers", {}) if isinstance(mapping, dict) else {}
    devs = mapping.get("developers", {}) if isinstance(mapping, dict) else {}

    def _tier_rank(t: str) -> int:
        tt = str(t or "").strip()
        if tt == "AAA":
            return 3
        if tt == "AA":
            return 2
        if tt == "Indie":
            return 1
        if tt == "Unknown":
            return 0
        return -1

    pubs_norm: dict[str, tuple[str, str]] = {}
    for k, v in pubs.items():
        nk = normalize_company_name(k)
        tier = str(v or "").strip()
        if not nk or not tier:
            continue
        key = nk.casefold()
        prev = pubs_norm.get(key)
        if prev is None or _tier_rank(tier) > _tier_rank(prev[0]):
            pubs_norm[key] = (tier, str(k))

    devs_norm: dict[str, tuple[str, str]] = {}
    for k, v in devs.items():
        nk = normalize_company_name(k)
        tier = str(v or "").strip()
        if not nk or not tier:
            continue
        key = nk.casefold()
        prev = devs_norm.get(key)
        if prev is None or _tier_rank(tier) > _tier_rank(prev[0]):
            devs_norm[key] = (tier, str(k))

    for pub in _split_csv_list(row.get("Steam_Publishers", "")):
        for pub_n in iter_company_name_variants(pub):
            if pub_n.startswith(("Feral Interactive", "Aspyr")):
                continue
            tier, key = pubs_norm.get(pub_n.casefold(), ("", ""))
            if tier and tier != "Unknown":
                return (tier, f"publisher:{key}")
    for dev in _split_csv_list(row.get("Steam_Developers", "")):
        for dev_n in iter_company_name_variants(dev):
            if dev_n.startswith(("Feral Interactive", "Aspyr")):
                continue
            tier, key = devs_norm.get(dev_n.casefold(), ("", ""))
            if tier and tier != "Unknown":
                return (tier, f"developer:{key}")
    return ("", "")


def apply_phase1_signals(
    df: pd.DataFrame,
    *,
    production_tiers_path: str | Path = "data/production_tiers.yaml",
) -> pd.DataFrame:
    """
    Add Phase-1 computed signals to the merged enriched dataframe.
    """
    out = df.copy()

    # --- Reach: SteamSpy owners ---
    lows: list[str] = []
    highs: list[str] = []
    mids: list[str] = []
    for v in out.get("SteamSpy_Owners", [""] * len(out)):
        low, high, mid = parse_steamspy_owners_range(v)
        lows.append(str(low) if low is not None else "")
        highs.append(str(high) if high is not None else "")
        mids.append(str(mid) if mid is not None else "")
    out["Reach_SteamSpyOwners_Low"] = pd.Series(lows)
    out["Reach_SteamSpyOwners_High"] = pd.Series(highs)
    out["Reach_SteamSpyOwners_Mid"] = pd.Series(mids)

    # Wikipedia pageviews are stored as provider fields (Wikidata_Pageviews*). We avoid creating
    # redundant Reach_/Now_ copies to keep the merged CSV lean; composites derive directly from
    # Wikidata_Pageviews* when present.

    # --- Reach composite (0..100) ---
    reach_comp: list[str] = []
    for _, r in out.iterrows():
        pairs: list[tuple[float, float]] = []

        owners_mid = _parse_int(r.get("Reach_SteamSpyOwners_Mid", ""))
        owners_score = _log_scale_0_100(
            owners_mid,
            log10_min=SIGNALS.reach_owners_log10_min,
            log10_max=SIGNALS.reach_owners_log10_max,
        )
        if owners_score is not None:
            pairs.append((owners_score, SIGNALS.w_owners))

        steam_reviews = _parse_int(r.get("Steam_ReviewCount", ""))
        reviews_score = _log_scale_0_100(
            steam_reviews,
            log10_min=SIGNALS.reach_reviews_log10_min,
            log10_max=SIGNALS.reach_reviews_log10_max,
        )
        if reviews_score is not None:
            pairs.append((reviews_score, SIGNALS.w_reviews))

        rawg_votes = _parse_int(r.get("RAWG_RatingsCount", ""))
        rawg_votes_score = _log_scale_0_100(
            rawg_votes,
            log10_min=SIGNALS.reach_votes_log10_min,
            log10_max=SIGNALS.reach_votes_log10_max,
        )
        if rawg_votes_score is not None:
            pairs.append((rawg_votes_score, SIGNALS.w_votes))

        igdb_votes = _parse_int(r.get("IGDB_RatingCount", ""))
        igdb_votes_score = _log_scale_0_100(
            igdb_votes,
            log10_min=SIGNALS.reach_votes_log10_min,
            log10_max=SIGNALS.reach_votes_log10_max,
        )
        if igdb_votes_score is not None:
            pairs.append((igdb_votes_score, SIGNALS.w_votes))

        igdb_critic_votes = _parse_int(r.get("IGDB_AggregatedRatingCount", ""))
        igdb_critic_votes_score = _log_scale_0_100(
            igdb_critic_votes,
            log10_min=SIGNALS.reach_critic_votes_log10_min,
            log10_max=SIGNALS.reach_critic_votes_log10_max,
        )
        if igdb_critic_votes_score is not None:
            pairs.append((igdb_critic_votes_score, SIGNALS.w_critic_votes))

        wiki_365 = _parse_int(r.get("Wikidata_Pageviews365d", ""))
        wiki_365_score = _log_scale_0_100(
            wiki_365,
            log10_min=SIGNALS.reach_pageviews_log10_min,
            log10_max=SIGNALS.reach_pageviews_log10_max,
        )
        if wiki_365_score is not None:
            pairs.append((wiki_365_score, SIGNALS.w_pageviews))

        avg = _weighted_avg(pairs)
        reach_comp.append(str(int(round(avg))) if avg is not None else "")
    out["Reach_Composite"] = pd.Series(reach_comp)

    # --- Ratings: community composite (0..100) ---
    community_scores: list[str] = []
    for _, r in out.iterrows():
        pairs: list[tuple[float, float]] = []

        # SteamSpy (percent derived from pos/neg)
        s = _parse_float(r.get("Score_SteamSpy_100", ""))
        if s is not None:
            pos = _parse_int(r.get("SteamSpy_Positive", ""))
            neg = _parse_int(r.get("SteamSpy_Negative", ""))
            w = _log_weight((pos or 0) + (neg or 0))
            pairs.append((float(s), w))

        # RAWG rating is 0..5
        rawg_rating = _parse_float(r.get("RAWG_Rating", ""))
        if rawg_rating is not None:
            rawg_count = _parse_int(r.get("RAWG_RatingsCount", ""))
            pairs.append((rawg_rating / 5.0 * 100.0, _log_weight(rawg_count)))

        # IGDB rating already in 0..100-ish; keep as-is
        igdb_score = _parse_float(r.get("Score_IGDB_100", ""))
        if igdb_score is not None:
            igdb_count = _parse_int(r.get("IGDB_RatingCount", ""))
            pairs.append((float(igdb_score), _log_weight(igdb_count)))

        # HLTB score is 0..100 but count is unknown; keep low weight.
        hltb_score = _parse_float(r.get("Score_HLTB_100", ""))
        if hltb_score is not None:
            pairs.append((float(hltb_score), 1.0))

        avg = _weighted_avg(pairs)
        community_scores.append(str(int(round(avg))) if avg is not None else "")

    out["CommunityRating_Composite_100"] = pd.Series(community_scores)

    # --- Ratings: critic composite (0..100) ---
    critic_scores: list[str] = []
    for _, r in out.iterrows():
        pairs: list[tuple[float, float]] = []

        steam_meta = _parse_float(r.get("Steam_Metacritic", ""))
        if steam_meta is not None and 0 <= steam_meta <= 100:
            pairs.append((float(steam_meta), 1.0))

        rawg_meta = _parse_float(r.get("RAWG_Metacritic", ""))
        if rawg_meta is not None and 0 <= rawg_meta <= 100:
            pairs.append((float(rawg_meta), 1.0))

        igdb_agg = _parse_float(r.get("IGDB_AggregatedRating", ""))
        if igdb_agg is not None and 0 <= igdb_agg <= 100:
            igdb_agg_count = _parse_int(r.get("IGDB_AggregatedRatingCount", ""))
            pairs.append((float(igdb_agg), _log_weight(igdb_agg_count)))

        avg = _weighted_avg(pairs)
        critic_scores.append(str(int(round(avg))) if avg is not None else "")
    out["CriticRating_Composite_100"] = pd.Series(critic_scores)

    # --- Normalized critic score from IGDB aggregated rating (0..100) ---
    igdb_critic_scores: list[str] = []
    for v in out.get("IGDB_AggregatedRating", [""] * len(out)):
        f = _parse_float(v)
        igdb_critic_scores.append(str(int(round(f))) if f is not None else "")
    out["Score_IGDBCritic_100"] = pd.Series(igdb_critic_scores)

    # --- Production tier (optional mapping) ---
    mapping = load_production_tiers(production_tiers_path)
    tiers: list[str] = []
    reasons: list[str] = []
    for _, r in out.iterrows():
        tier, reason = compute_production_tier(r.to_dict(), mapping)
        tiers.append(tier)
        reasons.append(reason)
    out["Production_Tier"] = pd.Series(tiers)
    out["Production_TierReason"] = pd.Series(reasons)

    # --- Reach convenience columns ---
    out["Reach_SteamReviews"] = out.get("Steam_ReviewCount", "")
    out["Reach_RAWGRatingsCount"] = out.get("RAWG_RatingsCount", "")
    out["Reach_IGDBRatingCount"] = out.get("IGDB_RatingCount", "")
    out["Reach_IGDBAggregatedRatingCount"] = out.get("IGDB_AggregatedRatingCount", "")

    # --- Now (current interest): SteamSpy activity proxies ---
    out["Now_SteamSpyPlaytimeAvg2Weeks"] = out.get("SteamSpy_PlaytimeAvg2Weeks", "")
    out["Now_SteamSpyPlaytimeMedian2Weeks"] = out.get("SteamSpy_PlaytimeMedian2Weeks", "")

    # --- Now composite (0..100) ---
    now_comp: list[str] = []
    for _, r in out.iterrows():
        pairs: list[tuple[float, float]] = []

        ccu = _parse_int(r.get("SteamSpy_CCU", ""))
        ccu_score = _log_scale_0_100(
            ccu,
            log10_min=SIGNALS.now_ccu_log10_min,
            log10_max=SIGNALS.now_ccu_log10_max,
        )
        if ccu_score is not None:
            pairs.append((ccu_score, SIGNALS.w_ccu))

        wiki_30 = _parse_int(r.get("Wikidata_Pageviews30d", ""))
        wiki_30_score = _log_scale_0_100(
            wiki_30,
            log10_min=SIGNALS.now_pageviews_log10_min,
            log10_max=SIGNALS.now_pageviews_log10_max,
        )
        if wiki_30_score is not None:
            pairs.append((wiki_30_score, SIGNALS.w_now_pageviews))

        avg = _weighted_avg(pairs)
        now_comp.append(str(int(round(avg))) if avg is not None else "")
    out["Now_Composite"] = pd.Series(now_comp)

    # --- Launch interest proxy (0..100, optional) ---
    # Uses Wikipedia pageviews in the first 90 days since release when available.
    launch_scores: list[str] = []
    for v in out.get("Wikidata_PageviewsFirst90d", [""] * len(out)):
        count = _parse_int(v)
        scaled = _log_scale_0_100(
            count,
            log10_min=SIGNALS.now_pageviews_log10_min,
            log10_max=SIGNALS.now_pageviews_log10_max,
        )
        launch_scores.append(str(int(round(scaled))) if scaled is not None else "")
    out["Launch_Interest_100"] = pd.Series(launch_scores)

    return out
