from __future__ import annotations

import logging
import math
import re
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import yaml

from ..config import SIGNALS
from ..metrics.registry import MetricsRegistry, load_metrics_registry
from .company import iter_company_name_variants, normalize_company_name
from .utilities import normalize_game_name


def _parse_int_text(value: Any) -> int | None:
    """
    Parse an integer from text.

    This is only used for provider fields that are inherently string-encoded (e.g. SteamSpy owners
    ranges like \"1,000 .. 2,000\"). Typed metric inputs should not require parsing.
    """
    s = str(value or "").strip()
    if not s:
        return None
    if s.casefold() in {"nan", "none", "null"}:
        return None
    s2 = re.sub(r"[,\s]", "", s)
    if s2.isdigit() or (s2.startswith("-") and s2[1:].isdigit()):
        try:
            return int(s2)
        except Exception:
            return None
    return None


def _parse_float_text(value: Any) -> float | None:
    s = str(value or "").strip()
    if not s:
        return None
    if s.casefold() in {"nan", "none", "null"}:
        return None
    try:
        f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _as_str_list(value: Any) -> list[str]:
    """
    Return a list of strings from a typed list cell (no CSV parsing).

    Internal pipeline code keeps list-like metrics as typed lists (from in-memory provider payloads
    and JSONL). CSV exports may render lists as joined strings, but those are not meant to be
    parsed back.
    """
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for it in value:
        s = str(it or "").strip()
        if s:
            out.append(s)
    return out


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

    low = _parse_int_text(m.group("low"))
    high = _parse_int_text(m.group("high"))
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


def _company_sets_by_provider(
    row: Mapping[str, Any], *, kind: str
) -> tuple[dict[str, set[str]], dict[str, dict[str, str]]]:
    """
    Extract normalized company-name sets from provider cells.

    Returns:
      - provider -> set(normalized_company_key)
      - provider -> {normalized_company_key -> original_name} (best-effort for display)
    """
    if kind not in {"developer", "publisher"}:
        raise ValueError("kind must be developer or publisher")

    keys_by_provider: dict[str, str] = {
        "steam": "steam.developers" if kind == "developer" else "steam.publishers",
        "rawg": "rawg.developers" if kind == "developer" else "rawg.publishers",
        "igdb": "igdb.developers" if kind == "developer" else "igdb.publishers",
        "wikidata": "wikidata.developers" if kind == "developer" else "wikidata.publishers",
    }

    sets: dict[str, set[str]] = {}
    originals: dict[str, dict[str, str]] = {}

    for prov, key in keys_by_provider.items():
        raw_list = _as_str_list(row.get(key, ""))
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


def _content_type_from_steam(row: Mapping[str, Any]) -> str:
    st = str(row.get("steam.store_type", "") or "").strip().lower()
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


def _content_type_from_igdb(row: Mapping[str, Any]) -> str:
    # Relationship-only heuristics (no extra endpoints/fields):
    # - version_parent implies this title is a version/edition/port of another
    # - parent_game implies this title is a child (DLC/expansion/etc)
    if str(row.get("igdb.relationships.version_parent", "") or "").strip():
        return "port"
    if str(row.get("igdb.relationships.parent_game", "") or "").strip():
        return "dlc"
    return ""


def _igdb_list_items(row: Mapping[str, Any], key: str) -> list[str]:
    return [x for x in _as_str_list(row.get(key, "")) if x]


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


def _igdb_related_counts(row: Mapping[str, Any]) -> tuple[int, int, int]:
    """
    Return (dlcs_non_soundtrack, expansions, ports).
    """
    dlcs = [x for x in _igdb_list_items(row, "igdb.relationships.dlcs") if not _looks_like_non_content_dlc(x)]
    expansions = _igdb_list_items(row, "igdb.relationships.expansions")
    ports = _igdb_list_items(row, "igdb.relationships.ports")
    return (len(dlcs), len(expansions), len(ports))


def _content_type_source_signals(row: Mapping[str, Any]) -> list[str]:
    """
    Return compact, human-readable tags describing the source signals.

    These are meant for review/debugging, not for strict parsing.
    """
    out: list[str] = []
    st = str(row.get("steam.store_type", "") or "").strip().lower()
    if st:
        out.append(f"steam:type={st}")
    if str(row.get("igdb.relationships.version_parent", "") or "").strip():
        out.append("igdb:version_parent")
    if str(row.get("igdb.relationships.parent_game", "") or "").strip():
        out.append("igdb:parent_game")
    dlc_n, exp_n, port_n = _igdb_related_counts(row)
    if dlc_n:
        out.append(f"igdb:dlcs={dlc_n}")
    if exp_n:
        out.append(f"igdb:expansions={exp_n}")
    if port_n:
        out.append(f"igdb:ports={port_n}")
    return out


def _content_type_consensus(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    """
    Compute (content_type, consensus_providers, source_signals, conflict) conservatively.

    - Uses only explicit signals:
      - steam.store_type
      - IGDB relationships (igdb.relationships.*)
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
    f = _parse_float_text(value)
    if f is None:
        return None
    if f <= 0:
        return None
    return float(f)


def _compute_replayability(row: Mapping[str, Any]) -> tuple[str, str]:
    """
    Return (Replayability_100, Replayability_SourceSignals).

    This is a conservative heuristic intended for sorting/triage, not a ground-truth label.
    """
    steam_cats = " ".join(_normalize_token(x) for x in _as_str_list(row.get("steam.categories")))
    steamspy_tags = " ".join(_normalize_token(x) for x in _as_str_list(row.get("steamspy.popularity.tags")))
    igdb_modes = " ".join(_normalize_token(x) for x in _as_str_list(row.get("igdb.game_modes")))
    rawg_tags = " ".join(_normalize_token(x) for x in _as_str_list(row.get("rawg.tags")))
    rawg_genres = " ".join(_normalize_token(x) for x in _as_str_list(row.get("rawg.genres")))
    igdb_genres = " ".join(_normalize_token(x) for x in _as_str_list(row.get("igdb.genres")))
    combined = " ".join(
        x for x in (steam_cats, steamspy_tags, igdb_modes, rawg_tags, rawg_genres, igdb_genres) if x
    )

    main = _parse_hltb_hours(row.get("hltb.time.main"))
    extra = _parse_hltb_hours(row.get("hltb.time.extra"))
    comp = _parse_hltb_hours(row.get("hltb.time.completionist"))

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
    has_coop = _has_any(
        "co op",
        "coop",
        "co-operative",
        "cooperative",
        "online co op",
        "local co op",
    )
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


def _compute_main_genre(row: Mapping[str, Any]) -> tuple[str, str]:
    """
    Return (Genre_Main, Genre_MainSources).

    Uses cross-provider genre lists when available, preferring consensus across providers.
    Falls back to first genre from a provider priority order.
    """
    by_provider: dict[str, list[str]] = {
        "igdb": _as_str_list(row.get("igdb.genres")),
        "rawg": _as_str_list(row.get("rawg.genres")),
        "wikidata": _as_str_list(row.get("wikidata.genres")),
        # Steam "tags" are genre-like and can help for PC-only titles.
        "steam": _as_str_list(row.get("steam.tags")),
    }
    by_provider = {p: [g for g in gs if str(g).strip()] for p, gs in by_provider.items() if gs}
    if not by_provider:
        return ("", "")

    norm_to_providers: dict[str, set[str]] = {}
    norm_to_label: dict[str, str] = {}
    for prov, genres in by_provider.items():
        for g in genres:
            label = str(g or "").strip()
            if not label:
                continue
            norm = normalize_game_name(label)
            if not norm:
                continue
            norm_to_providers.setdefault(norm, set()).add(prov)
            norm_to_label.setdefault(norm, label)

    if not norm_to_providers:
        return ("", "")

    def _best_norm() -> str:
        items = [(len(p), n) for n, p in norm_to_providers.items()]
        best_count = max(c for c, _ in items)
        # Prefer consensus (2+ providers) when possible.
        if best_count >= 2:
            bests = [n for c, n in items if c == best_count]
            return sorted(bests)[0]
        # No consensus: fall back to provider order + first-genre order.
        for prov in ("igdb", "rawg", "wikidata", "steam"):
            genres = by_provider.get(prov) or []
            for g in genres:
                norm = normalize_game_name(str(g or "").strip())
                if norm in norm_to_providers:
                    return norm
        return sorted(norm_to_providers.keys())[0]

    best = _best_norm()
    label = norm_to_label.get(best, "")
    sources = "+".join(sorted(norm_to_providers.get(best) or []))
    return (label, sources)


def _compute_modding_signal(row: Mapping[str, Any]) -> tuple[str, str, str]:
    """
    Return (HasWorkshop, ModdingSignal_100, Modding_SourceSignals).
    """
    cats = " ".join(_normalize_token(x) for x in _as_str_list(row.get("steam.categories")))
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
    row: Mapping[str, Any], mapping: Mapping[str, Mapping[str, object]]
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

    def _lookup(store: Mapping[str, object], key: str) -> tuple[str, str]:
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
            out.extend(_as_str_list(row.get(c, "")))
        return out

    publisher_cols = (
        "steam.publishers",
        "igdb.publishers",
        "rawg.publishers",
        "wikidata.publishers",
    )
    developer_cols = (
        "steam.developers",
        "igdb.developers",
        "rawg.developers",
        "wikidata.developers",
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
    registry: MetricsRegistry | None = None,
    metrics_registry_path: str | Path = "data/metrics-registry.yaml",
    production_tiers_path: str | Path = "data/production_tiers.yaml",
) -> pd.DataFrame:
    """
    Add Phase-1 computed signals to the merged enriched dataframe.
    """
    out = df.copy()
    reg = registry or load_metrics_registry(metrics_registry_path)

    mapping = load_production_tiers(production_tiers_path)
    pubs_map = mapping.get("publishers", {}) if isinstance(mapping, dict) else {}
    devs_map = mapping.get("developers", {}) if isinstance(mapping, dict) else {}
    if not pubs_map and not devs_map:
        company_keys = [
            "steam.publishers",
            "steam.developers",
            "rawg.publishers",
            "rawg.developers",
            "igdb.publishers",
            "igdb.developers",
            "wikidata.publishers",
            "wikidata.developers",
        ]
        has_company_data = False
        for key in company_keys:
            mapped = reg.column_for_key(key)
            if mapped is None:
                continue
            col, _typ = mapped
            if col not in out.columns:
                continue
            s = out[col]
            if bool(s.map(lambda v: isinstance(v, list) and len(v) > 0).any()):
                has_company_data = True
                break
        if has_company_data:
            logging.info(
                "Production tiers mapping is empty/missing; run "
                "`python run.py collect-production-tiers data/output/Games_Enriched.csv`, "
                "edit tiers in `data/production_tiers.yaml` (start from "
                "`cp data/production_tiers.example.yaml data/production_tiers.yaml`), "
                "then re-run `enrich`."
            )

    # Clear derived/composite columns up-front to avoid stale values on in-place enrich.
    for metric_key, (col, _typ) in reg.by_key.items():
        if metric_key.startswith(("derived.", "composite.")):
            if col not in out.columns:
                out[col] = ""
            else:
                out[col] = ""

    for idx, r in out.iterrows():
        metrics_row: dict[str, Any] = {}
        for col, value in r.items():
            if not isinstance(col, str):
                continue
            mapped = reg.key_for_column(col)
            if mapped is None:
                continue
            key, _typ = mapped
            metrics_row[key] = value

        metrics = compute_phase1_signal_metrics(metrics_row, production_tiers=mapping)
        for key, v in metrics.items():
            mapped = reg.column_for_key(key)
            if mapped is None:
                continue
            col, _typ = mapped
            if col not in out.columns:
                out[col] = ""
            out.at[idx, col] = v

    return out


def compute_phase1_signal_metrics(
    row: Mapping[str, Any], *, production_tiers: dict[str, dict[str, object]]
) -> dict[str, object]:
    """
    Compute Phase-1 derived/composite metrics for a single row.

    Returns a dict of dotted metric keys to typed values (int/bool/string/list).
    """

    out: dict[str, object] = {}

    low, high, mid = parse_steamspy_owners_range(row.get("steamspy.owners"))
    if low is not None:
        out["derived.reach.steamspy_owners_low"] = low
    if high is not None:
        out["derived.reach.steamspy_owners_high"] = high
    if mid is not None:
        out["derived.reach.steamspy_owners_mid"] = mid

    def _int(key: str) -> int | None:
        v = row.get(key, None)
        if v is None or isinstance(v, bool):
            return None
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                return None
            if v.is_integer():
                return int(v)
        return None

    def _float(key: str) -> float | None:
        v = row.get(key, None)
        if v is None or isinstance(v, bool):
            return None
        if isinstance(v, (int, float)):
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        return None

    # Convenience "reach" counters (typed, derived from provider fields).
    steam_reviews = _int("steam.review_count")
    if steam_reviews is not None:
        out["derived.reach.steam_reviews"] = steam_reviews
    rawg_votes = _int("rawg.ratings_count")
    if rawg_votes is not None:
        out["derived.reach.rawg_ratings_count"] = rawg_votes
    igdb_votes = _int("igdb.score_count")
    if igdb_votes is not None:
        out["derived.reach.igdb_rating_count"] = igdb_votes
    igdb_critic_votes = _int("igdb.critic_score_count")
    if igdb_critic_votes is not None:
        out["derived.reach.igdb_aggregated_rating_count"] = igdb_critic_votes

    # --- Reach composite (0..100) ---
    reach_pairs: list[tuple[float, float]] = []
    owners_score = _log_scale_0_100(
        mid,
        log10_min=SIGNALS.reach_owners_log10_min,
        log10_max=SIGNALS.reach_owners_log10_max,
    )
    if owners_score is not None:
        reach_pairs.append((owners_score, SIGNALS.w_owners))

    reviews_score = _log_scale_0_100(
        steam_reviews,
        log10_min=SIGNALS.reach_reviews_log10_min,
        log10_max=SIGNALS.reach_reviews_log10_max,
    )
    if reviews_score is not None:
        reach_pairs.append((reviews_score, SIGNALS.w_reviews))

    rawg_votes_score = _log_scale_0_100(
        rawg_votes,
        log10_min=SIGNALS.reach_votes_log10_min,
        log10_max=SIGNALS.reach_votes_log10_max,
    )
    if rawg_votes_score is not None:
        reach_pairs.append((rawg_votes_score, SIGNALS.w_votes))

    rawg_added = _int("rawg.popularity.added_total")
    if rawg_added is None:
        buckets = [
            _int("rawg.popularity.added_by_status.owned"),
            _int("rawg.popularity.added_by_status.playing"),
            _int("rawg.popularity.added_by_status.beaten"),
            _int("rawg.popularity.added_by_status.toplay"),
            _int("rawg.popularity.added_by_status.dropped"),
        ]
        if any(x is not None for x in buckets):
            rawg_added = int(sum(int(x or 0) for x in buckets))
    rawg_added_score = _log_scale_0_100(
        rawg_added,
        log10_min=SIGNALS.reach_rawg_added_log10_min,
        log10_max=SIGNALS.reach_rawg_added_log10_max,
    )
    if rawg_added_score is not None:
        reach_pairs.append((rawg_added_score, SIGNALS.w_rawg_added))

    igdb_votes_score = _log_scale_0_100(
        igdb_votes,
        log10_min=SIGNALS.reach_votes_log10_min,
        log10_max=SIGNALS.reach_votes_log10_max,
    )
    if igdb_votes_score is not None:
        reach_pairs.append((igdb_votes_score, SIGNALS.w_votes))

    igdb_critic_votes_score = _log_scale_0_100(
        igdb_critic_votes,
        log10_min=SIGNALS.reach_critic_votes_log10_min,
        log10_max=SIGNALS.reach_critic_votes_log10_max,
    )
    if igdb_critic_votes_score is not None:
        reach_pairs.append((igdb_critic_votes_score, SIGNALS.w_critic_votes))

    wiki_365 = _int("wikipedia.pageviews_365d")
    wiki_365_score = _log_scale_0_100(
        wiki_365,
        log10_min=SIGNALS.reach_pageviews_log10_min,
        log10_max=SIGNALS.reach_pageviews_log10_max,
    )
    if wiki_365_score is not None:
        reach_pairs.append((wiki_365_score, SIGNALS.w_pageviews))

    reach_avg = _weighted_avg(reach_pairs)
    if reach_avg is not None:
        out["composite.reach.score_100"] = int(round(reach_avg))

    # --- Ratings: community composite (0..100) ---
    comm_pairs: list[tuple[float, float]] = []

    steamspy_score = _float("steamspy.score_100")
    if steamspy_score is not None:
        pos = _int("steamspy.positive")
        neg = _int("steamspy.negative")
        comm_pairs.append((float(steamspy_score), _log_weight((pos or 0) + (neg or 0))))

    rawg_score = _float("rawg.score_100")
    if rawg_score is not None:
        comm_pairs.append((float(rawg_score), _log_weight(rawg_votes)))

    igdb_score = _float("igdb.score_100")
    if igdb_score is not None:
        comm_pairs.append((float(igdb_score), _log_weight(igdb_votes)))

    hltb_score = _float("hltb.score_100")
    if hltb_score is not None:
        comm_pairs.append((float(hltb_score), 1.0))

    comm_avg = _weighted_avg(comm_pairs)
    if comm_avg is not None:
        out["composite.community_rating.score_100"] = int(round(comm_avg))

    # --- Ratings: critic composite (0..100) ---
    critic_pairs: list[tuple[float, float]] = []

    steam_meta = _float("steam.metacritic_100")
    if steam_meta is not None and 0 <= steam_meta <= 100:
        critic_pairs.append((float(steam_meta), 1.0))

    rawg_meta = _float("rawg.metacritic_100")
    if rawg_meta is not None and 0 <= rawg_meta <= 100:
        critic_pairs.append((float(rawg_meta), 1.0))

    igdb_critic = _float("igdb.critic.score_100")
    if igdb_critic is not None and 0 <= igdb_critic <= 100:
        critic_pairs.append((float(igdb_critic), _log_weight(igdb_critic_votes)))

    critic_avg = _weighted_avg(critic_pairs)
    if critic_avg is not None:
        out["composite.critic_rating.score_100"] = int(round(critic_avg))

    # --- Developer/publisher consensus (derived, conservative) ---
    dev_sets, dev_originals = _company_sets_by_provider(row, kind="developer")
    pub_sets, pub_originals = _company_sets_by_provider(row, kind="publisher")

    dev_providers, dev_keys = _company_strict_majority_consensus(dev_sets)
    pub_providers, pub_keys = _company_strict_majority_consensus(pub_sets)

    if dev_providers:
        out["derived.companies.developers_consensus_providers"] = "+".join(dev_providers)
        out["derived.companies.developers_consensus_provider_count"] = len(dev_providers)
    if pub_providers:
        out["derived.companies.publishers_consensus_providers"] = "+".join(pub_providers)
        out["derived.companies.publishers_consensus_provider_count"] = len(pub_providers)

    def _display(
        keys: list[str], originals: dict[str, dict[str, str]], providers: tuple[str, ...]
    ) -> list[str]:
        if not keys or not providers:
            return []
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
        return out_list

    dev_names = _display(dev_keys, dev_originals, dev_providers)
    pub_names = _display(pub_keys, pub_originals, pub_providers)
    if dev_names:
        out["derived.companies.developers_consensus"] = dev_names
    if pub_names:
        out["derived.companies.publishers_consensus"] = pub_names

    # --- Content type (derived consensus) ---
    ct, prov, signals, conflict = _content_type_consensus(row)
    if ct:
        out["derived.content_type.value"] = ct
    if prov:
        out["derived.content_type.consensus_providers"] = prov
    if signals:
        out["derived.content_type.source_signals"] = signals
    if conflict:
        out["derived.content_type.conflict"] = conflict

    # --- IGDB related content presence (derived, filtered) ---
    dlc_n, exp_n, port_n = _igdb_related_counts(row)
    if dlc_n > 0:
        out["derived.igdb.has_dlcs"] = True
    if exp_n > 0:
        out["derived.igdb.has_expansions"] = True
    if port_n > 0:
        out["derived.igdb.has_ports"] = True

    # --- Main genre (derived) ---
    g, src = _compute_main_genre(row)
    if g:
        out["derived.genre.main"] = g
    if src:
        out["derived.genre.sources"] = src

    # --- Replayability & modding/UGC proxies (derived, best-effort) ---
    rep, rep_sig = _compute_replayability(row)
    rep_i = _parse_int_text(rep)
    if rep_i is not None:
        out["derived.replayability.score_100"] = rep_i
    if rep_sig:
        out["derived.replayability.source_signals"] = rep_sig

    hw, ms, ms_sig = _compute_modding_signal(row)
    if hw:
        out["derived.modding.has_workshop"] = True
    ms_i = _parse_int_text(ms)
    if ms_i is not None:
        out["derived.modding.score_100"] = ms_i
    if ms_sig:
        out["derived.modding.source_signals"] = ms_sig

    # --- Production tier (optional mapping) ---
    tier, reason = compute_production_tier(row, production_tiers)
    if tier:
        out["derived.production.tier"] = tier
    if reason:
        out["derived.production.tier_reason"] = reason

    # --- Now (current interest): SteamSpy activity proxies ---
    play_avg_2w = _int("steamspy.playtime_avg_2weeks")
    if play_avg_2w is not None:
        out["derived.now.steamspy_playtime_avg_2weeks"] = play_avg_2w
    play_med_2w = _int("steamspy.playtime_median_2weeks")
    if play_med_2w is not None:
        out["derived.now.steamspy_playtime_median_2weeks"] = play_med_2w

    now_pairs: list[tuple[float, float]] = []
    ccu = _int("steamspy.ccu")
    ccu_score = _log_scale_0_100(
        ccu,
        log10_min=SIGNALS.now_ccu_log10_min,
        log10_max=SIGNALS.now_ccu_log10_max,
    )
    if ccu_score is not None:
        now_pairs.append((ccu_score, SIGNALS.w_ccu))

    p2w = _int("steamspy.players_2weeks")
    p2w_score = _log_scale_0_100(
        p2w,
        log10_min=SIGNALS.now_players2w_log10_min,
        log10_max=SIGNALS.now_players2w_log10_max,
    )
    if p2w_score is not None:
        now_pairs.append((p2w_score, SIGNALS.w_players2w))

    wiki_30 = _int("wikipedia.pageviews_30d")
    wiki_30_score = _log_scale_0_100(
        wiki_30,
        log10_min=SIGNALS.now_pageviews_log10_min,
        log10_max=SIGNALS.now_pageviews_log10_max,
    )
    if wiki_30_score is not None:
        now_pairs.append((wiki_30_score, SIGNALS.w_now_pageviews))

    now_avg = _weighted_avg(now_pairs)
    if now_avg is not None:
        out["composite.now.score_100"] = int(round(now_avg))

    # --- Launch interest proxy (0..100, optional) ---
    first_90 = _int("wikipedia.pageviews_first_90d")
    launch_scaled = _log_scale_0_100(
        first_90,
        log10_min=SIGNALS.now_pageviews_log10_min,
        log10_max=SIGNALS.now_pageviews_log10_max,
    )
    if launch_scaled is not None:
        out["composite.launch_interest.score_100"] = int(round(launch_scaled))

    return out
