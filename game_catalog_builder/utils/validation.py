from __future__ import annotations

from dataclasses import dataclass

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
    "deluxe",
    "gold",
    "platinum",
}


_DLC_TOKENS = {
    "dlc",
    "soundtrack",
    "demo",
    "beta",
    "season",
    "pass",
    "expansion",
    "pack",
}


def _edition_tokens(title: str) -> set[str]:
    """
    Extract a small set of edition/port/remaster tokens from a title for cross-provider comparison.
    """
    t = normalize_game_name(title)
    tokens = set(t.split())
    out = {tok for tok in tokens if tok in _STEAM_EDITION_TOKENS}
    if "game of the year" in t:
        out.add("goty")
    if "director s cut" in t or "directors cut" in t:
        out.add("directors")
    return out


def _steam_looks_like_dlc(steam_name: str, steam_categories: str) -> bool:
    t = normalize_game_name(steam_name)
    tokens = set(t.split())
    if any(tok in tokens for tok in _DLC_TOKENS):
        return True
    cats = normalize_game_name(steam_categories)
    if "downloadable content" in cats:
        return True
    return False


def _contains_cyrillic(s: str) -> bool:
    for ch in s or "":
        o = ord(ch)
        # Cyrillic + Cyrillic Supplement blocks
        if 0x0400 <= o <= 0x052F:
            return True
    return False


def _series_numbers(title: str) -> set[int]:
    """
    Extract sequel/series numbers from a title, excluding likely years (1900-2100).

    Uses normalize_game_name() which already converts common roman numerals to digits.
    """
    tokens = normalize_game_name(title).split()
    out: set[int] = set()
    for i, t in enumerate(tokens):
        if not t.isdigit():
            continue
        # Ignore thousands-group patterns like "40,000" which normalize to "40 000".
        if i + 1 < len(tokens) and tokens[i + 1].isdigit() and tokens[i + 1] == "000":
            continue
        try:
            n = int(t)
        except ValueError:
            continue
        if n == 0:
            continue
        if 1900 <= n <= 2100:
            continue
        if 0 <= n <= 50:
            out.add(n)
    return out


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
    for k, s in (
        ("RAWG", score_rawg),
        ("IGDB", score_igdb),
        ("Steam", score_steam),
        ("HLTB", score_hltb),
    ):
        if s.strip().isdigit():
            scored.append((k, int(s)))
    if not scored:
        return ""
    scored.sort(key=lambda x: x[1])  # lowest first
    if scored[0][1] < threshold:
        return scored[0][0]
    return ""


def _year_diff(a: int | None, b: int | None) -> str:
    if a is None or b is None:
        return ""
    return str(a - b)


def _suggest_canonical_title(row: dict[str, str]) -> tuple[str, str, str, str, int, str]:
    """
    Returns:
        (canonical_title, canonical_source, suggested_personal_name, reason, consensus_count,
         consensus_sources)
    """
    candidates: list[tuple[str, str]] = []
    for src, col in (
        ("Steam", "Steam_Name"),
        ("RAWG", "RAWG_Name"),
        ("IGDB", "IGDB_Name"),
        ("HLTB", "HLTB_Name"),
    ):
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
        best_src = min(
            (preferred_order.index(src) for src, _ in items if src in preferred_order), default=999
        )
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
    thresholds: ValidationThresholds | None = None,
) -> pd.DataFrame:
    """
    Produce a per-row cross-provider consistency report for the merged CSV.
    """
    if thresholds is None:
        thresholds = ValidationThresholds()
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
        steam_is_dlc = _steam_looks_like_dlc(
            str(r.get("Steam_Name", "") or ""), str(r.get("Steam_Categories", "") or "")
        )

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
                # Steam years often represent ports/remasters/HD releases; lower severity if it
                # looks like an edition.
                if not steam_is_edition:
                    steam_year_disagree = "YES"

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

        # Edition token comparison across provider titles.
        edition_by_src = {
            "RAWG": _edition_tokens(rawg_name),
            "IGDB": _edition_tokens(igdb_name),
            "Steam": _edition_tokens(steam_name),
            "HLTB": _edition_tokens(hltb_name),
        }
        # Compare token sets across providers that have a title, treating "no tokens" as a signal
        # too (e.g. "Remastered" vs base game).
        titled_providers = {
            "RAWG": bool(rawg_name),
            "IGDB": bool(igdb_name),
            "Steam": bool(steam_name),
            "HLTB": bool(hltb_name),
        }
        compared = {k: v for k, v in edition_by_src.items() if titled_providers.get(k)}
        edition_disagree = ""
        if len({frozenset(v) for v in compared.values()}) >= 2:
            edition_disagree = "YES"
        edition_summary = "; ".join(
            f"{k}:{','.join(sorted(v))}" for k, v in edition_by_src.items() if v
        )

        # Non-English-ish signals: Cyrillic titles.
        cyrillic_prov: list[str] = []
        for prov, title in (
            ("RAWG", rawg_name),
            ("IGDB", igdb_name),
            ("Steam", steam_name),
            ("HLTB", hltb_name),
        ):
            if title and _contains_cyrillic(title):
                cyrillic_prov.append(prov)
        title_non_english = "YES" if cyrillic_prov else ""

        # Series number mismatch (e.g. "Assassin's Creed 2" vs "Assassin's Creed 3").
        series_by_src = {
            "RAWG": _series_numbers(rawg_name) if rawg_name else set(),
            "IGDB": _series_numbers(igdb_name) if igdb_name else set(),
            "Steam": _series_numbers(steam_name) if steam_name else set(),
            "HLTB": _series_numbers(hltb_name) if hltb_name else set(),
        }
        series_compared = {k: v for k, v in series_by_src.items() if v}
        series_disagree = ""
        if len(series_compared) >= 2 and len({frozenset(v) for v in series_compared.values()}) >= 2:
            series_disagree = "YES"
        series_summary = "; ".join(
            f"{k}:{','.join(str(x) for x in sorted(v))}" for k, v in series_by_src.items() if v
        )

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

        validation_tags: list[str] = []
        for prov in not_found:
            validation_tags.append(f"missing:{prov}")

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
                culprit = (
                    "IGDB"
                    if (score_steam and int(score_steam) >= thresholds.title_score_warn)
                    else "Steam"
                )
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
                if (
                    rawg_year is not None
                    and abs(steam_year - rawg_year) <= thresholds.year_max_diff
                ):
                    culprit = "IGDB"
                elif (
                    igdb_year is not None
                    and abs(steam_year - igdb_year) <= thresholds.year_max_diff
                ):
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

        if steam_is_dlc:
            culprit = "Steam"
        if not culprit and series_disagree == "YES":
            culprit = "RAWG/IGDB/Steam/HLTB"

        (
            canonical_title,
            canonical_source,
            suggested_personal,
            suggestion_reason,
            consensus_count,
            consensus_sources,
        ) = _suggest_canonical_title({k: str(v or "") for k, v in r.to_dict().items()})
        suggested_rename = ""
        review_title = "YES" if steam_is_dlc else ""
        review_reason = "steam looks like dlc/demo" if steam_is_dlc else ""
        if (
            canonical_title
            and name
            and normalize_game_name(name) != normalize_game_name(canonical_title)
        ):
            high_signal = any(
                x == "YES"
                for x in (
                    title_mismatch,
                    year_disagree_rawg_igdb,
                    platform_disagree,
                    steam_appid_mismatch,
                )
            )
            strong_crosscheck = bool(
                steam_appid and igdb_steam_appid and steam_appid == igdb_steam_appid
            )
            has_consensus = consensus_count >= 2
            if high_signal and (has_consensus or strong_crosscheck):
                suggested_rename = "YES"

            # Looser review list: consensus mismatches and Steam-ID-backed mismatches.
            if has_consensus:
                review_title = "YES"
                review_reason = f"provider consensus ({consensus_sources})"
            elif canonical_source == "Steam" and steam_appid:
                review_title = "YES"
                review_reason = "steam appid present"
            elif steam_appid_mismatch == "YES":
                review_title = "YES"
                review_reason = "steam appid mismatch"
            elif title_mismatch == "YES":
                review_title = "YES"
                review_reason = "title mismatch"

        if title_mismatch == "YES":
            validation_tags.append("title_mismatch")
        if year_disagree_rawg_igdb == "YES":
            validation_tags.append("year_disagree_rawg_igdb")
        if steam_year_disagree == "YES":
            validation_tags.append("steam_year_disagree")
        if platform_disagree == "YES":
            validation_tags.append("platform_disagree")
        if steam_appid_mismatch == "YES":
            validation_tags.append("steam_appid_mismatch")
        if edition_disagree == "YES":
            validation_tags.append("edition_disagree")
        if steam_is_dlc:
            validation_tags.append("steam_dlc_like")
        if series_disagree == "YES":
            validation_tags.append("series_disagree")
        if title_non_english == "YES":
            validation_tags.append("title_non_english")
        if suggested_rename == "YES":
            validation_tags.append("suggest_rename")
        if review_title == "YES":
            validation_tags.append("needs_review")

        steam_year_diff_vs_primary = ""
        if steam_year is not None:
            if igdb_year is not None:
                steam_year_diff_vs_primary = steam_year_diff_vs_igdb
            elif rawg_year is not None:
                steam_year_diff_vs_primary = steam_year_diff_vs_rawg

        rows.append(
            {
                "Name": name,
                "ValidationTags": ", ".join(validation_tags),
                "MissingProviders": ", ".join(not_found),
                "RAWG_Name": rawg_name,
                "IGDB_Name": igdb_name,
                "Steam_Name": steam_name,
                "HLTB_Name": hltb_name,
                "Years": "; ".join(f"{k}:{y}" for k, y in years),
                "SteamYearDiffVsPrimary": steam_year_diff_vs_primary,
                "EditionTokens": edition_summary,
                "SeriesNumbers": series_summary,
                "Platforms": "; ".join(f"{k}:{','.join(sorted(s))}" for k, s in non_empty),
                "PlatformIntersection": platform_intersection,
                "SteamAppID": steam_appid,
                "IGDB_SteamAppID": igdb_steam_appid,
                "SuggestedCulprit": culprit,
                "SuggestedCanonicalTitle": canonical_title,
                "SuggestedCanonicalSource": canonical_source,
                "ReviewTitle": review_title,
                "ReviewTitleReason": review_reason,
            }
        )

    return pd.DataFrame(rows)
