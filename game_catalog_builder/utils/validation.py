from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import pandas as pd

from .utilities import fuzzy_score, normalize_game_name


def _split_csv_list(s: str) -> list[str]:
    return [p.strip() for p in (s or "").split(",") if p.strip()]


def _normalize_platform_token(token: str) -> str:
    t = (token or "").strip().lower()
    if not t:
        return ""

    if "pc" in t or "windows" in t:
        return "pc"
    if "playstation" in t or t.startswith("ps"):
        return "playstation"
    if "xbox" in t:
        return "xbox"
    if "nintendo" in t or "switch" in t or "wii" in t:
        return "nintendo"
    if "mac" in t:
        return "mac"
    if "linux" in t:
        return "linux"
    return t


def _normalize_platforms(platforms: str) -> set[str]:
    out: set[str] = set()
    for token in _split_csv_list(platforms):
        norm = _normalize_platform_token(token)
        if norm:
            out.add(norm)
    return out


def _as_year_int(s: str) -> int | None:
    s = (s or "").strip()
    if len(s) != 4 or not s.isdigit():
        return None
    y = int(s)
    if 1900 <= y <= 2100:
        return y
    return None


@dataclass(frozen=True)
class ValidationThresholds:
    title_score_warn: int = 90
    year_max_diff: int = 1


_STEAM_EDITION_TOKENS = {
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
    "deluxe",
    "gold",
    "platinum",
}


def _steam_is_edition_or_port(steam_name: str) -> bool:
    tokens = set(normalize_game_name(steam_name).split())
    return any(t in tokens for t in _STEAM_EDITION_TOKENS)


def _pick_title_culprit(
    *,
    score_rawg: str,
    score_igdb: str,
    score_steam: str,
    score_hltb: str,
    threshold: int,
) -> str:
    scored: list[tuple[str, int]] = []
    for k, s in (("RAWG", score_rawg), ("IGDB", score_igdb), ("Steam", score_steam), ("HLTB", score_hltb)):
        if s.strip().isdigit():
            scored.append((k, int(s)))
    if not scored:
        return ""
    scored.sort(key=lambda x: x[1])  # lowest first
    if scored[0][1] < threshold:
        return scored[0][0]
    return ""


def _year_diff(a: Optional[int], b: Optional[int]) -> str:
    if a is None or b is None:
        return ""
    return str(a - b)


def _suggest_canonical_title(row: dict[str, str]) -> tuple[str, str, str, str, int, str]:
    """
    Returns (canonical_title, canonical_source, suggested_personal_name, reason, consensus_count, consensus_sources).
    """
    name = str(row.get("Name", "") or "").strip()

    candidates: list[tuple[str, str]] = []
    for src, col in (("Steam", "Steam_Name"), ("RAWG", "RAWG_Name"), ("IGDB", "IGDB_Name"), ("HLTB", "HLTB_Name")):
        t = str(row.get(col, "") or "").strip()
        if t:
            candidates.append((src, t))

    if not candidates:
        return "", "", "", "no provider titles available", 0, ""

    # Group by normalized title to find consensus.
    groups: dict[str, list[tuple[str, str]]] = {}
    for src, title in candidates:
        key = normalize_game_name(title)
        groups.setdefault(key, []).append((src, title))

    # Choose the largest group; tie-break by source preference.
    preferred_order = ["Steam", "RAWG", "IGDB", "HLTB"]

    def group_rank(items: list[tuple[str, str]]) -> tuple[int, int]:
        count = len(items)
        # best (lowest) source index present in the group.
        best_src = min((preferred_order.index(src) for src, _ in items if src in preferred_order), default=999)
        return (count, -best_src)

    best_key, best_items = sorted(groups.items(), key=lambda kv: group_rank(kv[1]), reverse=True)[0]

    # Representative title: prefer Steam if present in group, else other sources.
    rep_title = ""
    rep_source = ""
    for src in preferred_order:
        for s, t in best_items:
            if s == src:
                rep_source = s
                rep_title = t
                break
        if rep_title:
            break
    if not rep_title:
        rep_source, rep_title = best_items[0]

    # Suggested personal name: the canonical title.
    suggested_personal = rep_title

    reason = ""
    if len(best_items) >= 2:
        sources = sorted({s for s, _ in best_items})
        reason = "provider consensus: " + "+".join(sources)
    else:
        reason = f"single provider title ({rep_source})"

    consensus_sources = "+".join(sorted({s for s, _ in best_items})) if len(best_items) >= 2 else ""
    return rep_title, rep_source, suggested_personal, reason, len(best_items), consensus_sources


def generate_validation_report(
    df: pd.DataFrame,
    *,
    thresholds: ValidationThresholds = ValidationThresholds(),
) -> pd.DataFrame:
    """
    Produce a per-row cross-provider consistency report for the merged CSV.
    """
    rows: list[dict[str, str]] = []

    for _, r in df.iterrows():
        name = str(r.get("Name", "") or "").strip()

        rawg_name = str(r.get("RAWG_Name", "") or "").strip()
        igdb_name = str(r.get("IGDB_Name", "") or "").strip()
        steam_name = str(r.get("Steam_Name", "") or "").strip()
        hltb_name = str(r.get("HLTB_Name", "") or "").strip()

        score_rawg = str(fuzzy_score(name, rawg_name)) if rawg_name else ""
        score_igdb = str(fuzzy_score(name, igdb_name)) if igdb_name else ""
        score_steam = str(fuzzy_score(name, steam_name)) if steam_name else ""
        score_hltb = str(fuzzy_score(name, hltb_name)) if hltb_name else ""

        rawg_year = _as_year_int(str(r.get("RAWG_Year", "") or ""))
        igdb_year = _as_year_int(str(r.get("IGDB_Year", "") or ""))
        steam_year = _as_year_int(str(r.get("Steam_ReleaseYear", "") or ""))

        years: list[tuple[str, int]] = []
        if rawg_year is not None:
            years.append(("RAWG", rawg_year))
        if igdb_year is not None:
            years.append(("IGDB", igdb_year))
        if steam_year is not None:
            years.append(("Steam", steam_year))

        steam_is_edition = _steam_is_edition_or_port(steam_name)

        year_disagree_rawg_igdb = ""
        if rawg_year is not None and igdb_year is not None:
            if abs(rawg_year - igdb_year) > thresholds.year_max_diff:
                year_disagree_rawg_igdb = "YES"

        steam_year_disagree = ""
        steam_year_diff_vs_rawg = _year_diff(steam_year, rawg_year)
        steam_year_diff_vs_igdb = _year_diff(steam_year, igdb_year)
        if steam_year is not None:
            primary = igdb_year if igdb_year is not None else rawg_year
            if primary is not None and abs(steam_year - primary) > thresholds.year_max_diff:
                # Steam years often represent ports/remasters/HD releases; lower severity if it looks like an edition.
                if not steam_is_edition:
                    steam_year_disagree = "YES"

        # Backwards-compatible aggregate flag (now edition-aware for Steam).
        year_disagree = "YES" if (year_disagree_rawg_igdb or steam_year_disagree) else ""

        platforms = [
            ("RAWG", _normalize_platforms(str(r.get("RAWG_Platforms", "") or ""))),
            ("IGDB", _normalize_platforms(str(r.get("IGDB_Platforms", "") or ""))),
            ("Steam", _normalize_platforms(str(r.get("Steam_Platforms", "") or ""))),
        ]
        non_empty = [(k, s) for k, s in platforms if s]
        platform_disagree = ""
        platform_intersection = ""
        if len(non_empty) >= 2:
            inter = set.intersection(*(s for _, s in non_empty))
            platform_intersection = ", ".join(sorted(inter))
            if not inter:
                platform_disagree = "YES"

        steam_appid = str(r.get("Steam_AppID", "") or "").strip()
        igdb_steam_appid = str(r.get("IGDB_SteamAppID", "") or "").strip()
        steam_appid_mismatch = ""
        if steam_appid and igdb_steam_appid and steam_appid != igdb_steam_appid:
            steam_appid_mismatch = "YES"

        title_mismatch = ""
        for s in (score_rawg, score_igdb, score_steam, score_hltb):
            if s and int(s) < thresholds.title_score_warn:
                title_mismatch = "YES"
                break

        not_found: list[str] = []
        for prov, col in (
            ("RAWG", "RAWG_ID"),
            ("IGDB", "IGDB_ID"),
            ("Steam", "Steam_AppID"),
            ("HLTB", "HLTB_Main"),
            ("SteamSpy", "SteamSpy_Owners"),
        ):
            if not str(r.get(col, "") or "").strip():
                not_found.append(prov)

        culprit = ""
        if steam_appid_mismatch == "YES":
            # Use title scores as a tie-breaker for where the mismatch likely originates.
            title_culprit = _pick_title_culprit(
                score_rawg=score_rawg,
                score_igdb=score_igdb,
                score_steam=score_steam,
                score_hltb=score_hltb,
                threshold=thresholds.title_score_warn,
            )
            if title_culprit:
                culprit = title_culprit
            else:
                # Most often the IGDB match is wrong if Steam matched the input name well.
                culprit = "IGDB" if (score_steam and int(score_steam) >= thresholds.title_score_warn) else "Steam"
        elif platform_disagree == "YES":
            # A single odd platform set (e.g. web-only) is often a bad match.
            if "web browser" in str(r.get("IGDB_Platforms", "") or "").lower():
                culprit = "IGDB"
            else:
                culprit = _pick_title_culprit(
                    score_rawg=score_rawg,
                    score_igdb=score_igdb,
                    score_steam=score_steam,
                    score_hltb=score_hltb,
                    threshold=thresholds.title_score_warn,
                )
        elif year_disagree_rawg_igdb == "YES":
            # If Steam exists and strongly agrees with one year, blame the other.
            if steam_year is not None:
                if rawg_year is not None and abs(steam_year - rawg_year) <= thresholds.year_max_diff:
                    culprit = "IGDB"
                elif igdb_year is not None and abs(steam_year - igdb_year) <= thresholds.year_max_diff:
                    culprit = "RAWG"
            if not culprit:
                culprit = "RAWG/IGDB"
        elif title_mismatch == "YES":
            culprit = _pick_title_culprit(
                score_rawg=score_rawg,
                score_igdb=score_igdb,
                score_steam=score_steam,
                score_hltb=score_hltb,
                threshold=thresholds.title_score_warn,
            )

        canonical_title, canonical_source, suggested_personal, suggestion_reason, consensus_count, consensus_sources = _suggest_canonical_title(
            {k: str(v or "") for k, v in r.to_dict().items()}
        )
        suggested_rename = ""
        if canonical_title and name and normalize_game_name(name) != normalize_game_name(canonical_title):
            high_signal = any(
                x == "YES"
                for x in (
                    title_mismatch,
                    year_disagree_rawg_igdb,
                    platform_disagree,
                    steam_appid_mismatch,
                )
            )
            strong_crosscheck = bool(steam_appid and igdb_steam_appid and steam_appid == igdb_steam_appid)
            has_consensus = consensus_count >= 2
            if high_signal and (has_consensus or strong_crosscheck):
                suggested_rename = "YES"

        rows.append(
            {
                "Name": name,
                "MissingProviders": ", ".join(not_found),
                "RAWG_Name": rawg_name,
                "IGDB_Name": igdb_name,
                "Steam_Name": steam_name,
                "HLTB_Name": hltb_name,
                "Score_RAWG_vs_Personal": score_rawg,
                "Score_IGDB_vs_Personal": score_igdb,
                "Score_Steam_vs_Personal": score_steam,
                "Score_HLTB_vs_Personal": score_hltb,
                "Years": "; ".join(f"{k}:{y}" for k, y in years),
                "YearDisagree": year_disagree,
                "YearDisagree_RAWG_IGDB": year_disagree_rawg_igdb,
                "SteamYearDisagree": steam_year_disagree,
                "SteamYearDiff_vs_RAWG": steam_year_diff_vs_rawg,
                "SteamYearDiff_vs_IGDB": steam_year_diff_vs_igdb,
                "SteamEditionOrPort": "YES" if steam_is_edition else "",
                "Platforms": "; ".join(f"{k}:{','.join(sorted(s))}" for k, s in non_empty),
                "PlatformIntersection": platform_intersection,
                "PlatformDisagree": platform_disagree,
                "SteamAppID": steam_appid,
                "IGDB_SteamAppID": igdb_steam_appid,
                "SteamAppIDMismatch": steam_appid_mismatch,
                "TitleMismatch": title_mismatch,
                "SuggestedCulprit": culprit,
                "SuggestedCanonicalTitle": canonical_title,
                "SuggestedCanonicalSource": canonical_source,
                "SuggestedPersonalName": suggested_personal,
                "SuggestedRenamePersonalName": suggested_rename,
                "SuggestionReason": suggestion_reason,
                "CanonicalConsensusCount": str(consensus_count) if consensus_count else "",
                "CanonicalConsensusSources": consensus_sources,
            }
        )

    return pd.DataFrame(rows)
