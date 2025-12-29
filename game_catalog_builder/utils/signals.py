from __future__ import annotations

import json
import logging
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ..config import SIGNALS
from .company import iter_company_name_variants, normalize_company_name


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


def _split_text_list(value: Any) -> list[str]:
    """
    Parse a comma-separated list stored in a CSV cell.

    Examples:
      - "Action, Shooter"
      - "Single-player, Online Co-op"
    """
    raw = str(value or "").strip()
    if not raw:
        return []
    if raw.casefold() in {"nan", "none", "null"}:
        return []
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts


def _normalize_token(value: str) -> str:
    s = str(value or "").casefold().strip()
    if not s:
        return ""
    s = s.replace("&", " and ")
    s = re.sub(r"[\(\)\[\]\{\}]", " ", s)
    s = re.sub(r"[^a-z0-9\s:+/-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


_OWNERS_RANGE_RE = re.compile(r"^\s*(?P<low>[\d,\s]+)\s*(?:\.\.|-)\s*(?P<high>[\d,\s]+)\s*$")


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


def load_production_tiers(path: str | Path) -> dict[str, dict[str, object]]:
    """
    Load a manual production tiers YAML mapping.

    Expected format:
      publishers:
        "Company Name": {tier: "AAA|AA|Indie"}  # extra fields allowed
      developers:
        "Company Name": {tier: "AAA|AA|Indie"}

    Values may also be plain strings (tier).
    """
    p = Path(path)
    if not p.exists():
        return {"publishers": {}, "developers": {}}
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {"publishers": {}, "developers": {}}
    pubs_in = data.get("publishers") if isinstance(data, dict) else {}
    devs_in = data.get("developers") if isinstance(data, dict) else {}
    pubs_in = pubs_in if isinstance(pubs_in, dict) else {}
    devs_in = devs_in if isinstance(devs_in, dict) else {}

    def _tier(v: Any) -> str:
        if isinstance(v, dict):
            return str(v.get("tier") or "").strip()
        if isinstance(v, str):
            return v.strip()
        return ""

    pubs: dict[str, object] = {}
    devs: dict[str, object] = {}
    for label, v in pubs_in.items():
        tier = _tier(v)
        if not tier:
            continue
        n = normalize_company_name(label)
        if not n:
            continue
        pubs[n.casefold()] = {"tier": tier, "label": str(label or "").strip()}

    for label, v in devs_in.items():
        tier = _tier(v)
        if not tier:
            continue
        n = normalize_company_name(label)
        if not n:
            continue
        devs[n.casefold()] = {"tier": tier, "label": str(label or "").strip()}

    return {"publishers": pubs, "developers": devs}


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


def _company_sets_by_provider(
    row: dict[str, Any], *, kind: str
) -> tuple[dict[str, set[str]], dict[str, dict[str, str]]]:
    """
    Extract normalized company-name sets from provider cells.

    Returns:
      - provider -> set(normalized_company_key)
      - provider -> {normalized_company_key -> original_name} (best-effort for display)
    """
    if kind not in {"developer", "publisher"}:
        raise ValueError("kind must be developer or publisher")

    cols_by_provider: dict[str, str] = {
        "steam": "Steam_Developers" if kind == "developer" else "Steam_Publishers",
        "rawg": "RAWG_Developers" if kind == "developer" else "RAWG_Publishers",
        "igdb": "IGDB_Developers" if kind == "developer" else "IGDB_Publishers",
        "wikidata": "Wikidata_Developers" if kind == "developer" else "Wikidata_Publishers",
    }

    sets: dict[str, set[str]] = {}
    originals: dict[str, dict[str, str]] = {}

    for prov, col in cols_by_provider.items():
        raw_list = _split_csv_list(row.get(col, ""))
        prov_set: set[str] = set()
        prov_map: dict[str, str] = {}
        for raw in raw_list:
            n = normalize_company_name(raw)
            if not n:
                continue
            key = n.casefold()
            prov_set.add(key)
            prov_map.setdefault(key, raw)
        if prov_set:
            sets[prov] = prov_set
            originals[prov] = prov_map
    return sets, originals


def _company_strict_majority_consensus(
    company_sets: dict[str, set[str]],
) -> tuple[tuple[str, ...], list[str]]:
    """
    Conservative dev/pub consensus:
      - require a strict-majority overlap component, and
      - require a non-empty intersection across that component.
    """
    present = [p for p, s in company_sets.items() if s]
    if len(present) < 2:
        return (), []

    parent: dict[str, str] = {p: p for p in present}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i, a in enumerate(present):
        for b in present[i + 1 :]:
            if not company_sets[a].isdisjoint(company_sets[b]):
                union(a, b)

    comps: dict[str, set[str]] = {}
    for p in present:
        comps.setdefault(find(p), set()).add(p)
    groups = list(comps.values())
    groups.sort(key=lambda s: (-len(s), "+".join(sorted(s))))
    best = groups[0]
    if len(best) <= len(present) / 2:
        return (), []

    providers = tuple(sorted(best))
    inter = set.intersection(*(company_sets[p] for p in providers))
    if not inter:
        return (), []
    return providers, [x for x in sorted(inter)]


def _content_type_from_steam(row: dict[str, Any]) -> str:
    st = str(row.get("Steam_StoreType", "") or "").strip().lower()
    if not st:
        return ""
    if st == "game":
        return "base_game"
    if st == "dlc":
        return "dlc"
    if st == "demo":
        return "demo"
    if st == "soundtrack":
        return "soundtrack"
    if st == "bundle":
        return "collection"
    return ""


def _content_type_from_igdb(row: dict[str, Any]) -> str:
    # Relationship-only heuristics (no extra endpoints/fields):
    # - version_parent implies this title is a version/edition/port of another
    # - parent_game implies this title is a child (DLC/expansion/etc)
    if str(row.get("IGDB_VersionParent", "") or "").strip():
        return "port"
    if str(row.get("IGDB_ParentGame", "") or "").strip():
        return "dlc"
    return ""


def _igdb_list_items(row: dict[str, Any], col: str) -> list[str]:
    """
    IGDB relationship lists are stored as comma-separated strings in CSV output columns.
    """
    return [x for x in _split_text_list(row.get(col, "")) if x]


def _looks_like_non_content_dlc(title: str) -> bool:
    t = _normalize_token(title)
    if not t:
        return False

    # Common “non-game-content” DLC add-ons that aren’t helpful for content-type clarity.
    if "soundtrack" in t or "original soundtrack" in t:
        return True
    if re.search(r"\bost\b", t):
        return True
    if "artbook" in t or "art book" in t:
        return True
    if "wallpaper" in t or "wallpapers" in t:
        return True
    if re.search(r"\bmanual\b", t):
        return True
    if re.search(r"\bguide\b", t) and "strategy" in t:
        return True
    return False


def _igdb_related_counts(row: dict[str, Any]) -> tuple[int, int, int]:
    """
    Return (dlcs_non_soundtrack, expansions, ports).
    """
    dlcs = [x for x in _igdb_list_items(row, "IGDB_DLCs") if not _looks_like_non_content_dlc(x)]
    expansions = _igdb_list_items(row, "IGDB_Expansions")
    ports = _igdb_list_items(row, "IGDB_Ports")
    return (len(dlcs), len(expansions), len(ports))


def _content_type_source_signals(row: dict[str, Any]) -> list[str]:
    """
    Return compact, human-readable tags describing the source signals.

    These are meant for review/debugging, not for strict parsing.
    """
    out: list[str] = []
    st = str(row.get("Steam_StoreType", "") or "").strip().lower()
    if st:
        out.append(f"steam:type={st}")
    if str(row.get("IGDB_VersionParent", "") or "").strip():
        out.append("igdb:version_parent")
    if str(row.get("IGDB_ParentGame", "") or "").strip():
        out.append("igdb:parent_game")
    dlc_n, exp_n, port_n = _igdb_related_counts(row)
    if dlc_n:
        out.append(f"igdb:dlcs={dlc_n}")
    if exp_n:
        out.append(f"igdb:expansions={exp_n}")
    if port_n:
        out.append(f"igdb:ports={port_n}")
    return out


def _content_type_consensus(row: dict[str, Any]) -> tuple[str, str, str, str]:
    """
    Compute (content_type, consensus_providers, source_signals, conflict) conservatively.

    - Uses only explicit signals:
      - Steam_StoreType
      - IGDB relationships (VersionParent/ParentGame)
    - Returns empty content_type when providers disagree (no strict majority).
    """
    votes: list[tuple[str, str]] = []
    s = _content_type_from_steam(row)
    if s:
        votes.append(("steam", s))
    i = _content_type_from_igdb(row)
    if i:
        votes.append(("igdb", i))

    if not votes:
        return ("", "", ", ".join(_content_type_source_signals(row)), "")

    counts: dict[str, int] = {}
    providers_by_type: dict[str, list[str]] = {}
    for prov, v in votes:
        counts[v] = counts.get(v, 0) + 1
        providers_by_type.setdefault(v, []).append(prov)

    best_type, best_count = max(counts.items(), key=lambda kv: (kv[1], kv[0]))
    if best_count <= len(votes) / 2:
        # If we had multiple explicit votes but no majority, surface a conflict flag.
        conflict = "YES" if len(votes) >= 2 else ""
        return ("", "", ", ".join(_content_type_source_signals(row)), conflict)
    return (
        best_type,
        "+".join(sorted(providers_by_type.get(best_type, []))),
        ", ".join(_content_type_source_signals(row)),
        "",
    )


def _parse_hltb_hours(value: Any) -> float | None:
    """
    HLTB times are stored as numeric strings (hours).
    """
    f = _parse_float(value)
    if f is None:
        return None
    if f <= 0:
        return None
    return float(f)


def _compute_replayability(row: dict[str, Any]) -> tuple[str, str]:
    """
    Return (Replayability_100, Replayability_SourceSignals).

    This is a conservative heuristic intended for sorting/triage, not a ground-truth label.
    """
    steam_cats = " ".join(_normalize_token(x) for x in _split_text_list(row.get("Steam_Categories")))
    steamspy_tags = " ".join(_normalize_token(x) for x in _split_text_list(row.get("SteamSpy_Tags")))
    igdb_modes = " ".join(_normalize_token(x) for x in _split_text_list(row.get("IGDB_GameModes")))
    rawg_tags = " ".join(_normalize_token(x) for x in _split_text_list(row.get("RAWG_Tags")))
    rawg_genres = " ".join(_normalize_token(x) for x in _split_text_list(row.get("RAWG_Genres")))
    igdb_genres = " ".join(_normalize_token(x) for x in _split_text_list(row.get("IGDB_Genres")))
    combined = " ".join(
        x
        for x in (steam_cats, steamspy_tags, igdb_modes, rawg_tags, rawg_genres, igdb_genres)
        if x
    )

    main = _parse_hltb_hours(row.get("HLTB_Main"))
    extra = _parse_hltb_hours(row.get("HLTB_Extra"))
    comp = _parse_hltb_hours(row.get("HLTB_Completionist"))

    has_any_inputs = bool(combined.strip()) or any(v is not None for v in (main, extra, comp))
    if not has_any_inputs:
        return ("", "")

    def _has_any(*needles: str) -> bool:
        return any(n in combined for n in needles)

    has_multiplayer = _has_any(
        "multiplayer",
        "multi player",
        "online pvp",
        "online multiplayer",
        "pvp",
        "mmorpg",
        "massively multiplayer",
        "battle royale",
    )
    has_coop = _has_any("co op", "coop", "co-operative", "cooperative", "online co op", "local co op")
    has_pvp = _has_any("pvp", "competitive", "ranked", "versus")
    has_roguelike = _has_any("roguelike", "roguelite")
    has_procedural = _has_any("procedural", "procedurally generated", "procedural generation")
    has_sandbox = _has_any(
        "sandbox",
        "open world",
        "strategy",
        "4x",
        "simulation",
        "city builder",
        "management",
        "survival",
        "crafting",
        "building",
    )

    long_optional = False
    if main is not None and main > 0:
        if comp is not None and (comp / main) >= 2.0:
            long_optional = True
        if extra is not None and (extra / main) >= 1.5:
            long_optional = True

    score = 20.0
    signals: list[str] = []

    if has_multiplayer:
        score += 50.0
        signals.append("multiplayer")
    if has_coop:
        score += 20.0
        signals.append("coop")
    if has_pvp:
        score += 10.0
        signals.append("pvp")
    if has_roguelike:
        score += 25.0
        signals.append("roguelike")
    if has_procedural and "roguelike" not in signals:
        score += 15.0
        signals.append("procedural")
    if has_sandbox:
        score += 20.0
        signals.append("systemic")
    if long_optional:
        score += 10.0
        signals.append("optional_content")

    if score < 0:
        score = 0.0
    if score > 100:
        score = 100.0

    return (str(int(round(score))), ", ".join(signals))


def _compute_modding_signal(row: dict[str, Any]) -> tuple[str, str, str]:
    """
    Return (HasWorkshop, ModdingSignal_100, Modding_SourceSignals).
    """
    cats = " ".join(_normalize_token(x) for x in _split_text_list(row.get("Steam_Categories")))
    if not cats:
        return ("", "", "")

    has_workshop = "steam workshop" in cats
    has_level_editor = "level editor" in cats or "includes level editor" in cats
    has_mod_tools = "moddable" in cats or "mod tools" in cats or "mod support" in cats

    score = 0.0
    signals: list[str] = []
    if has_workshop:
        score = max(score, 90.0)
        signals.append("steam_workshop")
    if has_level_editor:
        score = max(score, 75.0)
        signals.append("level_editor")
    if has_mod_tools:
        score = max(score, 70.0)
        signals.append("mod_tools")

    if score <= 0:
        return ("", "0", "")
    return ("YES" if has_workshop else "", str(int(round(score))), ", ".join(signals))


def compute_production_tier(
    row: dict[str, Any], mapping: dict[str, dict[str, object]]
) -> tuple[str, str]:
    """
    Compute (tier, reason) using available developer/publisher names.

    Priority order:
      - publishers (Steam → IGDB → RAWG → Wikidata)
      - developers (Steam → IGDB → RAWG → Wikidata)

    If no non-Unknown tier is found but at least one company name is present,
    returns ("Unknown", "...") so the column isn't blank for known-company rows.
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

    def _lookup(store: dict[str, object], key: str) -> tuple[str, str]:
        """
        Return (tier, label) for a normalized key.

        Supports both:
          - new JSON object values: {"tier": "...", "label": "..."}
          - legacy plain string values: "AAA"
        """
        obj = store.get(key)
        if isinstance(obj, dict):
            tier = str(obj.get("tier") or "").strip()
            label = str(obj.get("label") or "").strip()
            return (tier, label)
        if isinstance(obj, str):
            return (obj.strip(), "")
        return ("", "")

    def _iter_company_field(*cols: str) -> list[str]:
        out: list[str] = []
        for c in cols:
            out.extend(_split_csv_list(row.get(c, "")))
        return out

    publisher_cols = (
        "Steam_Publishers",
        "IGDB_Publishers",
        "RAWG_Publishers",
        "Wikidata_Publishers",
    )
    developer_cols = (
        "Steam_Developers",
        "IGDB_Developers",
        "RAWG_Developers",
        "Wikidata_Developers",
    )

    saw_any_company = False
    saw_unknown: tuple[str, str] | None = None

    for pub in _iter_company_field(*publisher_cols):
        saw_any_company = True
        for pub_n in iter_company_name_variants(pub):
            if pub_n.casefold().startswith(("feral interactive", "aspyr")):
                continue
            tier, label = _lookup(pubs, pub_n.casefold())
            if tier and tier != "Unknown":
                return (tier, f"publisher:{label or pub}")
            if tier == "Unknown" and saw_unknown is None:
                saw_unknown = ("Unknown", f"publisher:{label or pub}")

    for dev in _iter_company_field(*developer_cols):
        saw_any_company = True
        for dev_n in iter_company_name_variants(dev):
            if dev_n.casefold().startswith(("feral interactive", "aspyr")):
                continue
            tier, label = _lookup(devs, dev_n.casefold())
            if tier and tier != "Unknown":
                return (tier, f"developer:{label or dev}")
            if tier == "Unknown" and saw_unknown is None:
                saw_unknown = ("Unknown", f"developer:{label or dev}")

    if saw_unknown is not None:
        return saw_unknown
    if saw_any_company:
        return ("Unknown", "")
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

        rawg_added = _parse_int(r.get("RAWG_Added", ""))
        if rawg_added is None:
            # Fall back to the sum of buckets when `added` isn't present (some RAWG payloads omit
            # it but still include `added_by_status`).
            buckets = [
                _parse_int(r.get("RAWG_AddedByStatusOwned", "")),
                _parse_int(r.get("RAWG_AddedByStatusPlaying", "")),
                _parse_int(r.get("RAWG_AddedByStatusBeaten", "")),
                _parse_int(r.get("RAWG_AddedByStatusToplay", "")),
                _parse_int(r.get("RAWG_AddedByStatusDropped", "")),
            ]
            if any(x is not None for x in buckets):
                rawg_added = int(sum(int(x or 0) for x in buckets))

        rawg_added_score = _log_scale_0_100(
            rawg_added,
            log10_min=SIGNALS.reach_rawg_added_log10_min,
            log10_max=SIGNALS.reach_rawg_added_log10_max,
        )
        if rawg_added_score is not None:
            pairs.append((rawg_added_score, SIGNALS.w_rawg_added))

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

    # --- Developer/publisher consensus (derived, conservative) ---
    dev_cons_prov: list[str] = []
    dev_cons_names: list[str] = []
    dev_cons_count: list[str] = []
    pub_cons_prov: list[str] = []
    pub_cons_names: list[str] = []
    pub_cons_count: list[str] = []

    for _, r in out.iterrows():
        row = r.to_dict()

        dev_sets, dev_originals = _company_sets_by_provider(row, kind="developer")
        pub_sets, pub_originals = _company_sets_by_provider(row, kind="publisher")

        dev_providers, dev_keys = _company_strict_majority_consensus(dev_sets)
        pub_providers, pub_keys = _company_strict_majority_consensus(pub_sets)

        dev_cons_prov.append("+".join(dev_providers) if dev_providers else "")
        pub_cons_prov.append("+".join(pub_providers) if pub_providers else "")
        dev_cons_count.append(str(len(dev_providers)) if dev_providers else "")
        pub_cons_count.append(str(len(pub_providers)) if pub_providers else "")

        def _display(
            keys: list[str], originals: dict[str, dict[str, str]], providers: tuple[str, ...]
        ) -> str:
            if not keys or not providers:
                return ""
            out_list: list[str] = []
            for k in keys:
                chosen = ""
                for prov in ("steam", "igdb", "rawg", "wikidata"):
                    if prov not in providers:
                        continue
                    chosen = str((originals.get(prov) or {}).get(k, "") or "").strip()
                    if chosen:
                        break
                out_list.append(chosen or k)
            return json.dumps(out_list, ensure_ascii=False)

        dev_cons_names.append(_display(dev_keys, dev_originals, dev_providers))
        pub_cons_names.append(_display(pub_keys, pub_originals, pub_providers))

    out["Developers_ConsensusProviders"] = pd.Series(dev_cons_prov)
    out["Developers_Consensus"] = pd.Series(dev_cons_names)
    out["Developers_ConsensusProviderCount"] = pd.Series(dev_cons_count)
    out["Publishers_ConsensusProviders"] = pd.Series(pub_cons_prov)
    out["Publishers_Consensus"] = pd.Series(pub_cons_names)
    out["Publishers_ConsensusProviderCount"] = pd.Series(pub_cons_count)

    # --- Content type (derived consensus) ---
    content_types: list[str] = []
    content_type_prov: list[str] = []
    content_type_signals: list[str] = []
    content_type_conflict: list[str] = []
    for _, r in out.iterrows():
        ct, prov, signals, conflict = _content_type_consensus(r.to_dict())
        content_types.append(ct)
        content_type_prov.append(prov)
        content_type_signals.append(signals)
        content_type_conflict.append(conflict)
    out["ContentType"] = pd.Series(content_types)
    out["ContentType_ConsensusProviders"] = pd.Series(content_type_prov)
    out["ContentType_SourceSignals"] = pd.Series(content_type_signals)
    out["ContentType_Conflict"] = pd.Series(content_type_conflict)

    # --- IGDB related content presence (derived, filtered) ---
    has_dlcs: list[str] = []
    has_exp: list[str] = []
    has_ports: list[str] = []
    for _, r in out.iterrows():
        dlc_n, exp_n, port_n = _igdb_related_counts(r.to_dict())
        has_dlcs.append("YES" if dlc_n > 0 else "")
        has_exp.append("YES" if exp_n > 0 else "")
        has_ports.append("YES" if port_n > 0 else "")
    out["HasDLCs"] = pd.Series(has_dlcs)
    out["HasExpansions"] = pd.Series(has_exp)
    out["HasPorts"] = pd.Series(has_ports)

    # --- Replayability & modding/UGC proxies (derived, best-effort) ---
    replayability: list[str] = []
    replayability_signals: list[str] = []
    has_workshop: list[str] = []
    modding_signal: list[str] = []
    modding_signals: list[str] = []
    for _, r in out.iterrows():
        rep, rep_sig = _compute_replayability(r.to_dict())
        replayability.append(rep)
        replayability_signals.append(rep_sig)
        hw, ms, ms_sig = _compute_modding_signal(r.to_dict())
        has_workshop.append(hw)
        modding_signal.append(ms)
        modding_signals.append(ms_sig)
    out["Replayability_100"] = pd.Series(replayability)
    out["Replayability_SourceSignals"] = pd.Series(replayability_signals)
    out["HasWorkshop"] = pd.Series(has_workshop)
    out["ModdingSignal_100"] = pd.Series(modding_signal)
    out["Modding_SourceSignals"] = pd.Series(modding_signals)

    # --- Production tier (optional mapping) ---
    mapping = load_production_tiers(production_tiers_path)
    pubs_map = mapping.get("publishers", {}) if isinstance(mapping, dict) else {}
    devs_map = mapping.get("developers", {}) if isinstance(mapping, dict) else {}
    if not pubs_map and not devs_map:
        # Best-effort hint: if Steam dev/pub exists but no tiers store, suggest the updater command.
        has_steam_companies = False
        for col in ("Steam_Publishers", "Steam_Developers"):
            if col in out.columns and bool(out[col].astype(str).str.strip().ne("").any()):
                has_steam_companies = True
                break
        if has_steam_companies:
            logging.info(
                "Production tiers mapping is empty/missing; run "
                "`python run.py collect-production-tiers data/output/Games_Enriched.csv`, "
                "edit tiers in `data/production_tiers.yaml` (start from "
                "`cp data/production_tiers.example.yaml data/production_tiers.yaml`), "
                "then re-run `enrich`."
            )
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
    out["Now_SteamSpyPlayers2Weeks"] = out.get("SteamSpy_Players2Weeks", "")
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

        p2w = _parse_int(r.get("SteamSpy_Players2Weeks", ""))
        p2w_score = _log_scale_0_100(
            p2w,
            log10_min=SIGNALS.now_players2w_log10_min,
            log10_max=SIGNALS.now_players2w_log10_max,
        )
        if p2w_score is not None:
            pairs.append((p2w_score, SIGNALS.w_players2w))

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
