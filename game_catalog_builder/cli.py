"""Command-line interface for game catalog builder."""

from __future__ import annotations

import argparse
import logging
import os
import re
import shlex
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from queue import Queue

import pandas as pd

from .clients import (
    HLTBClient,
    IGDBClient,
    RAWGClient,
    SteamClient,
    SteamSpyClient,
)
from .config import CLI, IGDB, MATCHING, RAWG, STEAM, STEAMSPY
from .utils import (
    IDENTITY_NOT_FOUND,
    PUBLIC_DEFAULT_COLS,
    ProjectPaths,
    ensure_columns,
    ensure_row_ids,
    extract_year_hint,
    fuzzy_score,
    generate_validation_report,
    is_row_processed,
    load_credentials,
    load_identity_overrides,
    merge_all,
    normalize_game_name,
    read_csv,
    write_csv,
)
from .utils.consistency import (
    actionable_mismatch_tags,
    compute_provider_consensus,
    platform_outlier_tags,
    year_outlier_tags,
)


def clear_prefixed_columns(df: pd.DataFrame, idx: int, prefix: str) -> None:
    for c in [col for col in df.columns if col.startswith(prefix)]:
        df.at[idx, c] = ""


EVAL_COLUMNS = [
    "RAWG_MatchedName",
    "RAWG_MatchScore",
    "RAWG_MatchedYear",
    "IGDB_MatchedName",
    "IGDB_MatchScore",
    "IGDB_MatchedYear",
    "Steam_MatchedName",
    "Steam_MatchScore",
    "Steam_MatchedYear",
    "HLTB_MatchedName",
    "HLTB_MatchScore",
    "HLTB_MatchedYear",
    "HLTB_MatchedPlatforms",
    "ReviewTags",
    "MatchConfidence",
    # Legacy column kept only so we can drop it from older CSVs.
    "NeedsReview",
]

DIAGNOSTIC_COLUMNS = [c for c in EVAL_COLUMNS if c != "NeedsReview"]


def drop_eval_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in EVAL_COLUMNS if c in df.columns]
    return df.drop(columns=cols) if cols else df


PROVIDER_PREFIXES = ("RAWG_", "IGDB_", "Steam_", "SteamSpy_", "HLTB_")
PINNED_ID_COLS = {"RAWG_ID", "IGDB_ID", "Steam_AppID", "HLTB_ID", "HLTB_Query"}


def build_personal_base_for_enrich(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the base dataframe for a fresh merge by removing provider-derived columns.

    This is critical for "in-place" enrich (input == output) to avoid keeping stale provider
    columns: merge_all is a left-join and does not overwrite existing same-named columns.
    """
    keep: list[str] = []
    for c in df.columns:
        if c in {"RowId", "Name"}:
            keep.append(c)
            continue
        if c in PINNED_ID_COLS:
            keep.append(c)
            continue
        if c in EVAL_COLUMNS:
            continue
        if c.startswith(PROVIDER_PREFIXES):
            continue
        keep.append(c)
    return df[keep].copy()


def _is_yes(v: object) -> bool:
    return str(v or "").strip().upper() in {"YES", "Y", "TRUE", "1"}


def _platform_is_pc_like(platform_value: object) -> bool:
    p = str(platform_value or "").strip().lower()
    if not p:
        return True
    return any(x in p for x in ("pc", "windows", "steam", "linux", "mac", "osx"))


def _extract_steam_appid_from_rawg(rawg_obj: object) -> str:
    if not isinstance(rawg_obj, dict):
        return ""
    stores = rawg_obj.get("stores")
    if not isinstance(stores, list):
        return ""
    for it in stores:
        if not isinstance(it, dict):
            continue
        url = str(it.get("url") or "").strip()
        if not url:
            continue
        m = re.search(r"/app/(\d+)\b", url)
        if m:
            return m.group(1)
    return ""


def fill_eval_tags(
    df: pd.DataFrame, *, sources: set[str] | None = None, clients: dict[str, object] | None = None
) -> pd.DataFrame:
    out = df.copy()
    out = ensure_columns(out, {"ReviewTags": "", "MatchConfidence": ""})

    tags_list: list[str] = []
    confidence_list: list[str] = []

    include_rawg = sources is None or "rawg" in sources
    include_igdb = sources is None or "igdb" in sources
    include_steam = sources is None or "steam" in sources
    include_hltb = sources is None or "hltb" in sources

    def _int_year(v: object) -> int | None:
        s = str(v or "").strip()
        if s.isdigit() and len(s) == 4:
            y = int(s)
            if 1900 <= y <= 2100:
                return y
        return None

    def _steam_year(details: object) -> int | None:
        if not isinstance(details, dict):
            return None
        date = str((details.get("release_date") or {}).get("date") or "")
        m = re.search(r"\b(19\d{2}|20\d{2})\b", date)
        if not m:
            return None
        try:
            return int(m.group(1))
        except ValueError:
            return None

    def _rawg_year(obj: object) -> int | None:
        if not isinstance(obj, dict):
            return None
        released = str(obj.get("released", "") or "").strip()
        if len(released) >= 4 and released[:4].isdigit():
            return int(released[:4])
        return None

    def _series_numbers(title: str) -> set[int]:
        tokens = normalize_game_name(title).split()
        out_set: set[int] = set()
        for i, t in enumerate(tokens):
            if not t.isdigit():
                continue
            # Ignore thousands-group patterns like "40,000" which normalize to "40 000".
            if i + 1 < len(tokens) and tokens[i + 1].isdigit() and tokens[i + 1] == "000":
                continue
            # Avoid leading-zero “brand” tokens like 007.
            if len(t) > 1 and t.startswith("0"):
                continue
            n = int(t)
            if n == 0:
                continue
            if 1900 <= n <= 2100:
                continue
            if 0 < n <= 50:
                out_set.add(n)
        return out_set

    def _platform_bucket(name: str) -> str | None:
        n = normalize_game_name(name)
        if any(x in n for x in ("pc", "windows", "mac", "osx", "linux")):
            return "pc"
        if "playstation" in n or n.startswith("ps"):
            return "playstation"
        if "xbox" in n:
            return "xbox"
        if any(x in n for x in ("nintendo", "switch", "wii")):
            return "nintendo"
        if any(x in n for x in ("ios", "android", "mobile")):
            return "mobile"
        return None

    def _platforms_from_csv_list(s: str) -> set[str]:
        out_set: set[str] = set()
        for part in [p.strip() for p in s.split(",") if p.strip()]:
            b = _platform_bucket(part)
            if b:
                out_set.add(b)
        return out_set

    def _platforms_from_rawg(obj: object) -> set[str]:
        if not isinstance(obj, dict):
            return set()
        out_set: set[str] = set()
        for it in obj.get("platforms", []) or []:
            if not isinstance(it, dict):
                continue
            pname = (it.get("platform") or {}).get("name")
            if not pname:
                continue
            b = _platform_bucket(str(pname))
            if b:
                out_set.add(b)
        return out_set

    def _platforms_from_hltb(obj: object) -> set[str]:
        if not isinstance(obj, dict):
            return set()
        return _platforms_from_csv_list(str(obj.get("HLTB_Platforms", "") or ""))

    def _genres_from_rawg(obj: object) -> set[str]:
        if not isinstance(obj, dict):
            return set()
        out_set: set[str] = set()
        for g in obj.get("genres", []) or []:
            if not isinstance(g, dict):
                continue
            name = str(g.get("name", "") or "").strip()
            if name:
                out_set.add(normalize_game_name(name))
        return out_set

    def _genres_from_csv_list(s: str) -> set[str]:
        out_set: set[str] = set()
        for part in [p.strip() for p in str(s or "").split(",") if p.strip()]:
            out_set.add(normalize_game_name(part))
        return {x for x in out_set if x}

    def _platforms_from_steam(details: object) -> set[str]:
        if not isinstance(details, dict):
            return set()
        plats = details.get("platforms") or {}
        if not isinstance(plats, dict):
            return set()
        out_set: set[str] = set()
        if plats.get("windows") or plats.get("mac") or plats.get("linux"):
            out_set.add("pc")
        return out_set

    for _, row in out.iterrows():
        tags: list[str] = []
        disabled = _is_yes(row.get("Disabled", ""))
        if disabled:
            tags.append("disabled")

        has_missing_provider = False
        has_medium_issue = False
        has_low_issue = False

        name = str(row.get("Name", "") or "").strip()
        year_hint = _int_year(row.get("YearHint", "")) or _int_year(row.get("Year", ""))

        if include_rawg:
            rawg_id = str(row.get("RAWG_ID", "") or "").strip()
            if rawg_id == IDENTITY_NOT_FOUND:
                tags.append("rawg_not_found")
            elif not rawg_id:
                tags.append("missing_rawg")
                has_missing_provider = True
            elif not str(row.get("RAWG_MatchedName", "") or "").strip():
                tags.append("rawg_id_unresolved")
                has_low_issue = True

        if include_igdb:
            igdb_id = str(row.get("IGDB_ID", "") or "").strip()
            if igdb_id == IDENTITY_NOT_FOUND:
                tags.append("igdb_not_found")
            elif not igdb_id:
                tags.append("missing_igdb")
                has_missing_provider = True
            elif not str(row.get("IGDB_MatchedName", "") or "").strip():
                tags.append("igdb_id_unresolved")
                has_low_issue = True

        if include_steam:
            steam_id = str(row.get("Steam_AppID", "") or "").strip()
            steam_expected = _platform_is_pc_like(row.get("Platform", ""))
            if steam_id == IDENTITY_NOT_FOUND:
                tags.append("steam_not_found")
            elif not steam_id and steam_expected:
                tags.append("missing_steam")
                has_missing_provider = True
            elif steam_id and not str(row.get("Steam_MatchedName", "") or "").strip():
                tags.append("steam_id_unresolved")
                has_low_issue = True

        if include_hltb:
            hltb_id = str(row.get("HLTB_ID", "") or "").strip()
            hltb_query = str(row.get("HLTB_Query", "") or "").strip()
            hltb_name = str(row.get("HLTB_MatchedName", "") or "").strip()
            if hltb_id == IDENTITY_NOT_FOUND or hltb_query == IDENTITY_NOT_FOUND:
                tags.append("hltb_not_found")
            elif not hltb_name:
                tags.append("missing_hltb")
                has_missing_provider = True

        for score_col, tag_prefix, enabled in (
            ("RAWG_MatchScore", "rawg_score", include_rawg),
            ("IGDB_MatchScore", "igdb_score", include_igdb),
            ("Steam_MatchScore", "steam_score", include_steam),
            ("HLTB_MatchScore", "hltb_score", include_hltb),
        ):
            if not enabled:
                continue
            s = str(row.get(score_col, "") or "").strip()
            if s.isdigit() and int(s) < 100:
                tags.append(f"{tag_prefix}:{s}")
                score = int(s)
                # Treat large title mismatches as low-confidence even if the provider returned a
                # candidate (e.g. Diablo vs Diablo IV).
                if score < 80:
                    has_low_issue = True
                elif score < 95:
                    has_medium_issue = True

        # High signal metadata checks (requires cached provider payloads).
        years: dict[str, int] = {}
        platforms: dict[str, set[str]] = {}
        genres: dict[str, set[str]] = {}

        if clients and isinstance(clients, dict):
            if include_rawg:
                rawg_id = str(row.get("RAWG_ID", "") or "").strip()
                if rawg_id and rawg_id != IDENTITY_NOT_FOUND:
                    rawg_client = clients.get("rawg")
                    rawg_obj = rawg_client.get_by_id(rawg_id) if rawg_client else None
                    y = _rawg_year(rawg_obj)
                    if y is not None:
                        years["rawg"] = y
                    platforms["rawg"] = _platforms_from_rawg(rawg_obj)
                    genres["rawg"] = _genres_from_rawg(rawg_obj)

            if include_igdb:
                igdb_id = str(row.get("IGDB_ID", "") or "").strip()
                if igdb_id and igdb_id != IDENTITY_NOT_FOUND:
                    igdb_client = clients.get("igdb")
                    igdb_obj = igdb_client.get_by_id(igdb_id) if igdb_client else None
                    if isinstance(igdb_obj, dict):
                        y = _int_year(igdb_obj.get("IGDB_Year", ""))
                        if y is not None:
                            years["igdb"] = y
                        platforms["igdb"] = _platforms_from_csv_list(
                            str(igdb_obj.get("IGDB_Platforms", "") or "")
                        )
                        genres["igdb"] = _genres_from_csv_list(
                            str(igdb_obj.get("IGDB_Genres", "") or "")
                        )

            if include_steam:
                steam_id = str(row.get("Steam_AppID", "") or "").strip()
                if steam_id and steam_id != IDENTITY_NOT_FOUND:
                    steam_client = clients.get("steam")
                    try:
                        appid = int(steam_id)
                    except ValueError:
                        appid = None
                    details = (
                        steam_client.get_app_details(appid) if steam_client and appid else None
                    )
                    y = _steam_year(details)
                    if y is not None:
                        years["steam"] = y
                    platforms["steam"] = _platforms_from_steam(details)

            if include_hltb:
                hltb_id = str(row.get("HLTB_ID", "") or "").strip()
                if hltb_id and hltb_id != IDENTITY_NOT_FOUND:
                    hltb_client = clients.get("hltb")
                    hltb_obj = hltb_client.get_by_id(hltb_id) if hltb_client else None
                    if isinstance(hltb_obj, dict):
                        y = _int_year(hltb_obj.get("HLTB_ReleaseYear", ""))
                        if y is not None:
                            years["hltb"] = y
                        platforms["hltb"] = _platforms_from_hltb(hltb_obj)

            # Cross-provider Steam AppID disagreements:
            # - IGDB can expose a Steam uid under external_games.
            # - RAWG can expose Steam /app/<appid> in store URLs.
            if include_steam:
                steam_id = str(row.get("Steam_AppID", "") or "").strip()
                if steam_id and steam_id != IDENTITY_NOT_FOUND:
                    igdb_id = str(row.get("IGDB_ID", "") or "").strip()
                    if include_igdb and igdb_id and igdb_id != IDENTITY_NOT_FOUND:
                        igdb_client = clients.get("igdb")
                        igdb_obj = igdb_client.get_by_id(igdb_id) if igdb_client else None
                        igdb_steam = str((igdb_obj or {}).get("IGDB_SteamAppID") or "").strip()
                        if igdb_steam and igdb_steam.isdigit() and igdb_steam != steam_id:
                            tags.append("steam_appid_disagree:igdb")
                            has_low_issue = True

                    rawg_id = str(row.get("RAWG_ID", "") or "").strip()
                    if include_rawg and rawg_id and rawg_id != IDENTITY_NOT_FOUND:
                        rawg_client = clients.get("rawg")
                        rawg_obj = rawg_client.get_by_id(rawg_id) if rawg_client else None
                        rawg_steam = _extract_steam_appid_from_rawg(rawg_obj)
                        if rawg_steam and rawg_steam.isdigit() and rawg_steam != steam_id:
                            tags.append("steam_appid_disagree:rawg")
                            has_low_issue = True

        if year_hint is not None and years:
            for prov, y in years.items():
                drift = abs(y - year_hint)
                # Steam release year is often a port/re-release year; only treat it as a strong
                # mismatch when other high-signal checks also disagree (e.g. series numbers).
                if prov == "steam":
                    if drift >= 10:
                        tags.append("steam_year_far")
                        has_medium_issue = True
                    elif drift >= 2:
                        tags.append("steam_year_drift")
                        has_medium_issue = True
                    continue
                if drift >= 2:
                    tags.append(f"year_hint_far:{prov}")
                    has_low_issue = True

        # Prefer cross-provider year checks using RAWG/IGDB (original release years). Steam year is
        # frequently later even for correct matches.
        if "rawg" in years and "igdb" in years:
            if abs(years["rawg"] - years["igdb"]) >= 2:
                tags.append("year_disagree")
                has_low_issue = True

        if "rawg" in genres and "igdb" in genres and genres["rawg"] and genres["igdb"]:
            if genres["rawg"].isdisjoint(genres["igdb"]):
                tags.append("genre_disagree")
                has_medium_issue = True

        # Symmetric year outlier tags relative to a strict-majority consensus year.
        year_tags = year_outlier_tags(years, max_diff=1)
        if year_tags:
            tags.extend(year_tags)
            if "year_no_consensus" in year_tags:
                has_low_issue = True
            else:
                has_low_issue = True

        # Symmetric platform outlier tags relative to strict-majority platform consensus.
        plat_tags = platform_outlier_tags(platforms)
        if plat_tags:
            tags.extend(plat_tags)
            if "platform_no_consensus" in plat_tags:
                has_low_issue = True
            else:
                has_low_issue = True

        provider_titles: dict[str, str] = {
            "rawg": str(row.get("RAWG_MatchedName", "") or "").strip() if include_rawg else "",
            "igdb": str(row.get("IGDB_MatchedName", "") or "").strip() if include_igdb else "",
            "steam": str(row.get("Steam_MatchedName", "") or "").strip() if include_steam else "",
            "hltb": str(row.get("HLTB_MatchedName", "") or "").strip() if include_hltb else "",
        }
        consensus = compute_provider_consensus(provider_titles, years=years)
        if consensus:
            tags.extend(consensus.tags())
            if not consensus.has_majority:
                has_low_issue = True
            elif consensus.outliers:
                has_medium_issue = True

        actionable = actionable_mismatch_tags(
            provider_consensus=consensus,
            years=years,
            year_tags=year_tags,
            platform_tags=plat_tags,
        )
        if actionable:
            tags.extend(actionable)
            has_low_issue = True

        # (platform outliers are handled above via platform_outlier_tags)

        if include_steam:
            steam_name = str(row.get("Steam_MatchedName", "") or "").strip()
            steam_id = str(row.get("Steam_AppID", "") or "").strip()
            if steam_id and steam_id != IDENTITY_NOT_FOUND and name and steam_name:
                q_nums = _series_numbers(name)
                s_nums = _series_numbers(steam_name)
                if q_nums != s_nums:
                    tags.append("steam_series_mismatch")
                    has_low_issue = True

        if disabled:
            confidence = ""
        elif has_low_issue:
            confidence = "LOW"
        elif has_missing_provider or has_medium_issue:
            confidence = "MEDIUM"
        else:
            confidence = "HIGH"

        tags_list.append(", ".join(tags))
        confidence_list.append(confidence)

    out["ReviewTags"] = pd.Series(tags_list)
    out["MatchConfidence"] = pd.Series(confidence_list)
    if "NeedsReview" in out.columns:
        out = out.drop(columns=["NeedsReview"])
    return out


def _parse_sources(
    raw: str, *, allowed: set[str], aliases: dict[str, list[str]] | None = None
) -> list[str]:
    """
    Parse a provider list string like:
      - "all"
      - "core"
      - "igdb,rawg,steam"

    Returns a de-duplicated list preserving order.
    """
    s = str(raw or "").strip()
    if not s:
        raise SystemExit("Missing --source value")

    tokens = [t.strip().lower() for t in s.split(",") if t.strip()]
    out: list[str] = []
    seen: set[str] = set()

    def _add(x: str) -> None:
        if x in seen:
            return
        seen.add(x)
        out.append(x)

    if len(tokens) == 1 and tokens[0] in {"all"}:
        for x in sorted(allowed):
            _add(x)
        return out

    aliases = aliases or {}
    for t in tokens:
        if t in aliases:
            for x in aliases[t]:
                if x not in allowed:
                    raise SystemExit(f"Unknown provider in alias '{t}': {x}")
                _add(x)
            continue
        if t not in allowed:
            raise SystemExit(
                f"Unknown provider: {t}. Allowed: {', '.join(sorted(allowed | set(aliases)))}"
            )
        _add(t)
    return out


def load_or_merge_dataframe(input_csv: Path, output_csv: Path) -> pd.DataFrame:
    """
    Load dataframe from input CSV, merging in existing data from output CSV if it exists.

    This ensures we always process all games from the input, while preserving
    already-processed data from previous runs.
    """
    # Always read from input_csv to get all games
    df = read_csv(input_csv)

    # If output_csv exists, merge its data to preserve already-processed games
    if output_csv.exists():
        df_output = read_csv(output_csv)
        # Prefer stable RowId merges when available; fall back to Name.
        if "RowId" in df.columns and "RowId" in df_output.columns:
            df = df.merge(df_output, on="RowId", how="left", suffixes=("", "_existing"))
        else:
            # Merge on Name, keeping data from output_csv where it exists.
            # If names repeat, merge using (Name, per-name occurrence) to avoid cartesian growth.
            if "Name" in df.columns and "Name" in df_output.columns:
                if df["Name"].duplicated().any() or df_output["Name"].duplicated().any():
                    df["__occ"] = df.groupby("Name").cumcount()
                    df_output["__occ"] = df_output.groupby("Name").cumcount()
                    df = df.merge(
                        df_output, on=["Name", "__occ"], how="left", suffixes=("", "_existing")
                    )
                    df = df.drop(columns=["__occ"])
                else:
                    df = df.merge(df_output, on="Name", how="left", suffixes=("", "_existing"))
            else:
                df = df.merge(df_output, on="Name", how="left", suffixes=("", "_existing"))
        # Drop duplicate columns from merge
        for col in df.columns:
            if col.endswith("_existing"):
                original_col = col.replace("_existing", "")
                if original_col in df.columns:
                    # Use existing value if available and non-empty, otherwise use original
                    # Handle both NaN and empty strings
                    mask = (df[col].notna()) & (df[col] != "")
                    df.loc[mask, original_col] = df.loc[mask, col]
                df = df.drop(columns=[col])

    df = ensure_columns(df, PUBLIC_DEFAULT_COLS)
    return df


def process_steam_and_steamspy_streaming(
    input_csv: Path,
    steam_output_csv: Path,
    steamspy_output_csv: Path,
    steam_cache_path: Path,
    steamspy_cache_path: Path,
    identity_overrides: dict[str, dict[str, str]] | None = None,
) -> None:
    """
    Run Steam + SteamSpy in a streaming pipeline.

    Steam discovers appids and pushes (name, appid) into a queue; SteamSpy consumes appids as soon
    as they are available, without waiting for Steam to finish the whole file.
    """
    steam_client = SteamClient(
        cache_path=steam_cache_path, min_interval_s=STEAM.storesearch_min_interval_s
    )
    steamspy_client = SteamSpyClient(
        cache_path=steamspy_cache_path, min_interval_s=STEAMSPY.min_interval_s
    )

    df_steam = load_or_merge_dataframe(input_csv, steam_output_csv)
    df_steamspy = read_csv(input_csv)
    df_steamspy = ensure_columns(df_steamspy, PUBLIC_DEFAULT_COLS)

    q: Queue[tuple[int, str, str] | None] = Queue()

    def steam_producer() -> None:
        processed = 0
        pending: dict[int, list[int]] = {}

        def _flush_pending() -> None:
            nonlocal processed
            if not pending:
                return
            appids = list(pending.keys())
            details_map = steam_client.get_app_details_many(appids)
            for appid, indices in list(pending.items()):
                details = details_map.get(appid)
                if not details:
                    continue
                fields = steam_client.extract_fields(int(appid), details)
                for idx2 in indices:
                    for k, v in fields.items():
                        df_steam.at[idx2, k] = v
                processed += len(indices)
            pending.clear()

        for idx, row in df_steam.iterrows():
            name = str(row.get("Name", "") or "").strip()
            if not name:
                continue

            rowid = str(row.get("RowId", "") or "").strip()
            override_appid = ""
            if identity_overrides and rowid:
                override_appid = str(
                    identity_overrides.get(rowid, {}).get("Steam_AppID", "") or ""
                ).strip()

            if override_appid == IDENTITY_NOT_FOUND:
                clear_prefixed_columns(df_steam, int(idx), "Steam_")
                continue

            # Consider Steam processed only when core fetched fields are present; appid alone is
            # insufficient for enrichment (import may pre-fill IDs).
            if is_row_processed(df_steam, int(idx), ["Steam_Name"]):
                current_appid = str(df_steam.at[idx, "Steam_AppID"] or "").strip()
                # If Steam is already processed, still enqueue for SteamSpy when SteamSpy is
                # missing.
                if current_appid and not is_row_processed(
                    df_steamspy, int(idx), ["SteamSpy_Owners"]
                ):
                    q.put((int(idx), name, current_appid))
                if override_appid and current_appid != override_appid:
                    pass
                else:
                    continue

            if override_appid:
                appid = override_appid
            else:
                logging.info(f"[STEAM] Processing: {name}")
                search = steam_client.search_appid(name)
                if not search:
                    continue
                appid = str(search.get("id") or "").strip()
                if not appid:
                    continue

            # Persist appid early so SteamSpy can start immediately.
            df_steam.at[idx, "Steam_AppID"] = appid
            q.put((int(idx), name, appid))

            try:
                appid_int = int(appid)
            except ValueError:
                continue

            cached_details = steam_client.get_app_details(appid_int)
            if cached_details:
                fields = steam_client.extract_fields(appid_int, cached_details)
                for k, v in fields.items():
                    df_steam.at[idx, k] = v
                processed += 1
            else:
                pending.setdefault(appid_int, []).append(int(idx))

            if processed % 10 == 0:
                base_cols = [c for c in ("RowId", "Name") if c in df_steam.columns]
                steam_cols = base_cols + [c for c in df_steam.columns if c.startswith("Steam_")]
                write_csv(df_steam[steam_cols], steam_output_csv)

            if len(pending) >= CLI.steam_streaming_flush_batch_size:
                _flush_pending()
                base_cols = [c for c in ("RowId", "Name") if c in df_steam.columns]
                steam_cols = base_cols + [c for c in df_steam.columns if c.startswith("Steam_")]
                write_csv(df_steam[steam_cols], steam_output_csv)

        _flush_pending()

        base_cols = [c for c in ("RowId", "Name") if c in df_steam.columns]
        steam_cols = base_cols + [c for c in df_steam.columns if c.startswith("Steam_")]
        write_csv(df_steam[steam_cols], steam_output_csv)
        q.put(None)

    def steamspy_consumer() -> None:
        processed = 0
        while True:
            item = q.get()
            if item is None:
                break

            idx, name, appid = item

            if is_row_processed(df_steamspy, idx, ["SteamSpy_Owners"]):
                continue

            logging.info(f"[STEAMSPY] {name} (AppID {appid})")
            data = steamspy_client.fetch(int(appid))
            if not data:
                continue

            for k, v in data.items():
                df_steamspy.at[idx, k] = v

            processed += 1
            if processed % 10 == 0:
                base_cols = [c for c in ("RowId", "Name") if c in df_steamspy.columns]
                steamspy_cols = base_cols + [
                    c for c in df_steamspy.columns if c.startswith("SteamSpy_")
                ]
                if "Score_SteamSpy_100" in df_steamspy.columns:
                    steamspy_cols.append("Score_SteamSpy_100")
                write_csv(df_steamspy[steamspy_cols], steamspy_output_csv)

        base_cols = [c for c in ("RowId", "Name") if c in df_steamspy.columns]
        steamspy_cols = base_cols + [c for c in df_steamspy.columns if c.startswith("SteamSpy_")]
        if "Score_SteamSpy_100" in df_steamspy.columns:
            steamspy_cols.append("Score_SteamSpy_100")
        write_csv(df_steamspy[steamspy_cols], steamspy_output_csv)

    with ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(steam_producer)
        f2 = executor.submit(steamspy_consumer)
        f1.result()
        f2.result()

    logging.info(f"[STEAM] Cache stats: {steam_client.format_cache_stats()}")
    logging.info(f"[STEAMSPY] Cache stats: {steamspy_client.format_cache_stats()}")


def process_igdb(
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    credentials: dict,
    required_cols: list[str],
    language: str = "en",
    identity_overrides: dict[str, dict[str, str]] | None = None,
) -> None:
    """Process games with IGDB data."""
    client = IGDBClient(
        client_id=credentials.get("igdb", {}).get("client_id", ""),
        client_secret=credentials.get("igdb", {}).get("client_secret", ""),
        cache_path=cache_path,
        language=language,
        min_interval_s=IGDB.min_interval_s,
    )

    df = load_or_merge_dataframe(input_csv, output_csv)

    processed = 0

    pending_by_id: dict[str, list[int]] = {}

    def _flush_pending() -> None:
        nonlocal processed
        if not pending_by_id:
            return
        ids = list(pending_by_id.keys())
        by_id = client.get_by_ids(ids)
        for igdb_id, indices in list(pending_by_id.items()):
            data = by_id.get(str(igdb_id))
            if not data:
                continue
            for idx2 in indices:
                for k, v in data.items():
                    df.at[idx2, k] = v
            processed += len(indices)
        pending_by_id.clear()

    for idx, row in df.iterrows():
        name = row.get("Name", "").strip()
        if not name:
            continue

        rowid = str(row.get("RowId", "") or "").strip()
        override_id = ""
        if identity_overrides and rowid:
            override_id = str(identity_overrides.get(rowid, {}).get("IGDB_ID", "") or "").strip()

        if override_id == IDENTITY_NOT_FOUND:
            clear_prefixed_columns(df, int(idx), "IGDB_")
            continue

        if is_row_processed(df, idx, required_cols):
            if override_id and str(df.at[idx, "IGDB_ID"] or "").strip() != override_id:
                pass
            else:
                continue

        if override_id:
            df.at[idx, "IGDB_ID"] = str(override_id).strip()
            pending_by_id.setdefault(str(override_id).strip(), []).append(int(idx))
        else:
            igdb_id = str(df.at[idx, "IGDB_ID"] or "").strip()
            if igdb_id:
                pending_by_id.setdefault(igdb_id, []).append(int(idx))
            else:
                logging.info(f"[IGDB] Processing: {name}")
                data = client.search(name)
                if not data:
                    continue
                for k, v in data.items():
                    df.at[idx, k] = v
                processed += 1

        if processed % 10 == 0:
            # Save only Name + IGDB columns
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            igdb_cols = base_cols + [c for c in df.columns if c.startswith("IGDB_")]
            if "Score_IGDB_100" in df.columns:
                igdb_cols.append("Score_IGDB_100")
            write_csv(df[igdb_cols], output_csv)

        if len(pending_by_id) >= CLI.igdb_flush_batch_size:
            _flush_pending()
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            igdb_cols = base_cols + [c for c in df.columns if c.startswith("IGDB_")]
            if "Score_IGDB_100" in df.columns:
                igdb_cols.append("Score_IGDB_100")
            write_csv(df[igdb_cols], output_csv)

    _flush_pending()

    # Save only Name + IGDB columns
    base_cols = [c for c in ("RowId", "Name") if c in df.columns]
    igdb_cols = base_cols + [c for c in df.columns if c.startswith("IGDB_")]
    if "Score_IGDB_100" in df.columns:
        igdb_cols.append("Score_IGDB_100")
    write_csv(df[igdb_cols], output_csv)
    logging.info(f"[IGDB] Cache stats: {client.format_cache_stats()}")
    logging.info(f"✔ IGDB completed: {output_csv}")


def process_rawg(
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    credentials: dict,
    required_cols: list[str],
    language: str = "en",
    identity_overrides: dict[str, dict[str, str]] | None = None,
) -> None:
    """Process games with RAWG data."""
    client = RAWGClient(
        api_key=credentials.get("rawg", {}).get("api_key", ""),
        cache_path=cache_path,
        language=language,
        min_interval_s=RAWG.min_interval_s,
    )

    df = load_or_merge_dataframe(input_csv, output_csv)

    processed = 0
    for idx, row in df.iterrows():
        name = row.get("Name", "").strip()
        if not name:
            continue

        rowid = str(row.get("RowId", "") or "").strip()
        override_id = ""
        if identity_overrides and rowid:
            override_id = str(identity_overrides.get(rowid, {}).get("RAWG_ID", "") or "").strip()

        if override_id == IDENTITY_NOT_FOUND:
            clear_prefixed_columns(df, int(idx), "RAWG_")
            continue

        if is_row_processed(df, idx, required_cols):
            if override_id and str(df.at[idx, "RAWG_ID"] or "").strip() != override_id:
                pass
            else:
                continue

        if override_id:
            result = client.get_by_id(override_id)
            if not result:
                logging.warning(f"[RAWG] Override id not found: {name} (RAWG_ID {override_id})")
                continue
        else:
            logging.info(f"[RAWG] Processing: {name}")
            result = client.search(name)
            if not result:
                continue

        fields = client.extract_fields(result)
        for k, v in fields.items():
            df.at[idx, k] = v

        processed += 1
        if processed % 10 == 0:
            # Save only Name + RAWG columns
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            rawg_cols = base_cols + [c for c in df.columns if c.startswith("RAWG_")]
            if "Score_RAWG_100" in df.columns:
                rawg_cols.append("Score_RAWG_100")
            write_csv(df[rawg_cols], output_csv)

    # Save only Name + RAWG columns
    base_cols = [c for c in ("RowId", "Name") if c in df.columns]
    rawg_cols = base_cols + [c for c in df.columns if c.startswith("RAWG_")]
    if "Score_RAWG_100" in df.columns:
        rawg_cols.append("Score_RAWG_100")
    write_csv(df[rawg_cols], output_csv)
    logging.info(f"[RAWG] Cache stats: {client.format_cache_stats()}")
    logging.info(f"✔ RAWG completed: {output_csv}")


def process_steam(
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    required_cols: list[str],
    identity_overrides: dict[str, dict[str, str]] | None = None,
) -> None:
    """Process games with Steam data."""
    client = SteamClient(
        cache_path=cache_path,
        min_interval_s=STEAM.storesearch_min_interval_s,
    )

    df = load_or_merge_dataframe(input_csv, output_csv)

    pending: dict[int, list[int]] = {}

    def _flush_pending() -> None:
        if not pending:
            return
        appids = list(pending.keys())
        details_by_id = client.get_app_details_many(appids)
        for appid, idxs in list(pending.items()):
            details = details_by_id.get(appid)
            if not isinstance(details, dict):
                continue
            fields = client.extract_fields(appid, details)
            for idx in idxs:
                for k, v in fields.items():
                    df.at[idx, k] = v
        pending.clear()

    queued = 0
    for idx, row in df.iterrows():
        name = str(row.get("Name", "") or "").strip()
        if not name:
            continue

        rowid = str(row.get("RowId", "") or "").strip()
        override_appid = ""
        if identity_overrides and rowid:
            override_appid = str(
                identity_overrides.get(rowid, {}).get("Steam_AppID", "") or ""
            ).strip()

        if override_appid == IDENTITY_NOT_FOUND:
            clear_prefixed_columns(df, int(idx), "Steam_")
            continue

        if is_row_processed(df, idx, required_cols):
            if override_appid and str(df.at[idx, "Steam_AppID"] or "").strip() != override_appid:
                pass
            else:
                continue

        appid_str = override_appid or str(row.get("Steam_AppID", "") or "").strip()
        if not appid_str:
            logging.info(f"[STEAM] Processing: {name}")
            search = client.search_appid(name)
            if not search or not search.get("id"):
                continue
            appid_str = str(search.get("id") or "").strip()
            df.at[idx, "Steam_AppID"] = appid_str
        try:
            appid = int(appid_str)
        except ValueError:
            continue

        pending.setdefault(appid, []).append(int(idx))
        queued += 1

        if queued % 10 == 0:
            # Save only Name + Steam columns
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            steam_cols = base_cols + [c for c in df.columns if c.startswith("Steam_")]
            write_csv(df[steam_cols], output_csv)

        if len(pending) >= CLI.steam_flush_batch_size:
            _flush_pending()
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            steam_cols = base_cols + [c for c in df.columns if c.startswith("Steam_")]
            write_csv(df[steam_cols], output_csv)

    _flush_pending()

    # Save only Name + Steam columns
    base_cols = [c for c in ("RowId", "Name") if c in df.columns]
    steam_cols = base_cols + [c for c in df.columns if c.startswith("Steam_")]
    write_csv(df[steam_cols], output_csv)
    logging.info(f"[STEAM] Cache stats: {client.format_cache_stats()}")
    logging.info(f"✔ Steam completed: {output_csv}")


def process_steamspy(
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    required_cols: list[str],
) -> None:
    """Process games with SteamSpy data."""
    client = SteamSpyClient(
        cache_path=cache_path,
        min_interval_s=STEAMSPY.min_interval_s,
    )

    if not input_csv.exists():
        error_msg = f"{input_csv} not found. Run steam processing first."
        logging.error(error_msg)
        raise FileNotFoundError(error_msg)

    df = load_or_merge_dataframe(input_csv, output_csv)

    processed = 0
    for idx, row in df.iterrows():
        appid = row.get("Steam_AppID", "").strip()
        name = row.get("Name", "").strip()

        if not appid:
            continue

        if is_row_processed(df, idx, required_cols):
            continue

        logging.info(f"[STEAMSPY] {name} (AppID {appid})")

        data = client.fetch(int(appid))
        if not data:
            logging.warning(f"  ↳ No data in SteamSpy: {name} (AppID {appid})")
            continue

        for k, v in data.items():
            df.at[idx, k] = v

        processed += 1
        if processed % 10 == 0:
            # Save only Name + SteamSpy columns
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            steamspy_cols = base_cols + [c for c in df.columns if c.startswith("SteamSpy_")]
            if "Score_SteamSpy_100" in df.columns:
                steamspy_cols.append("Score_SteamSpy_100")
            write_csv(df[steamspy_cols], output_csv)

    # Save only Name + SteamSpy columns
    base_cols = [c for c in ("RowId", "Name") if c in df.columns]
    steamspy_cols = base_cols + [c for c in df.columns if c.startswith("SteamSpy_")]
    if "Score_SteamSpy_100" in df.columns:
        steamspy_cols.append("Score_SteamSpy_100")
    write_csv(df[steamspy_cols], output_csv)
    logging.info(f"[STEAMSPY] Cache stats: {client.format_cache_stats()}")
    logging.info(f"✔ SteamSpy completed: {output_csv}")


def process_hltb(
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    required_cols: list[str],
    identity_overrides: dict[str, dict[str, str]] | None = None,
) -> None:
    """Process games with HowLongToBeat data."""
    client = HLTBClient(cache_path=cache_path)

    df = load_or_merge_dataframe(input_csv, output_csv)

    processed = 0
    for idx, row in df.iterrows():
        name = row.get("Name", "").strip()
        if not name:
            continue

        rowid = str(row.get("RowId", "") or "").strip()
        query = ""
        pinned_id = ""
        if identity_overrides and rowid:
            pinned_id = str(identity_overrides.get(rowid, {}).get("HLTB_ID", "") or "").strip()
            query = str(identity_overrides.get(rowid, {}).get("HLTB_Query", "") or "").strip()
        if pinned_id == IDENTITY_NOT_FOUND or query == IDENTITY_NOT_FOUND:
            clear_prefixed_columns(df, int(idx), "HLTB_")
            continue
        query = query or name

        if is_row_processed(df, idx, required_cols):
            prev_name = str(df.at[idx, "HLTB_Name"] or "").strip()
            if prev_name and normalize_game_name(prev_name) == normalize_game_name(query):
                continue

        logging.info(f"[HLTB] Processing: {query}")
        data = client.search(name, query=query, hltb_id=pinned_id or None)
        if not data:
            continue

        for k, v in data.items():
            df.at[idx, k] = v

        processed += 1
        if processed % 10 == 0:
            # Save only Name + HLTB columns
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            hltb_cols = base_cols + [c for c in df.columns if c.startswith("HLTB_")]
            if "Score_HLTB_100" in df.columns:
                hltb_cols.append("Score_HLTB_100")
            write_csv(df[hltb_cols], output_csv)

    # Save only Name + HLTB columns
    base_cols = [c for c in ("RowId", "Name") if c in df.columns]
    hltb_cols = base_cols + [c for c in df.columns if c.startswith("HLTB_")]
    if "Score_HLTB_100" in df.columns:
        hltb_cols.append("Score_HLTB_100")
    write_csv(df[hltb_cols], output_csv)
    logging.info(f"[HLTB] Cache stats: {client.format_cache_stats()}")
    logging.info(f"✔ HLTB completed: {output_csv}")


def setup_logging(log_file: Path) -> None:
    """Configure logging to both console and file."""
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Create formatters
    file_formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    console_formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # File handler
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Silence verbose HTTP debug logs by default
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    logging.info(f"Logging to file: {log_file}")


def _common_paths() -> tuple[Path, ProjectPaths]:
    project_root = Path(__file__).resolve().parent.parent
    paths = ProjectPaths.from_root(project_root)
    paths.ensure()
    return project_root, paths


def _is_under_dir(path: Path | None, root: Path) -> bool:
    if path is None:
        return False
    try:
        root_r = root.resolve()
        p_r = path.resolve()
        return p_r == root_r or root_r in p_r.parents
    except Exception:
        return False


def _default_log_file(
    paths: ProjectPaths, *, command_name: str, logs_dir: Path | None = None
) -> Path:
    logs_dir = logs_dir or paths.data_logs
    logs_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    stamp = now.strftime("%Y%m%d-%H%M%S") + f".{now.microsecond // 1000:03d}"
    base = f"log-{stamp}-{command_name}.log"
    candidate = logs_dir / base
    if not candidate.exists():
        return candidate

    for i in range(2, 1000):
        p = logs_dir / f"log-{stamp}-{command_name}-{i}.log"
        if not p.exists():
            return p
    return logs_dir / f"log-{stamp}-{command_name}-{os.getpid()}.log"


def _setup_logging_from_args(
    paths: ProjectPaths,
    log_file: Path | None,
    debug: bool,
    *,
    command_name: str,
    input_path: Path | None = None,
) -> None:
    default_logs_dir: Path | None = None
    if input_path is not None:
        try:
            exp_root = paths.data_experiments.resolve()
            inp = input_path.resolve()
            if exp_root == inp or exp_root in inp.parents:
                default_logs_dir = paths.data_experiments_logs
        except Exception:
            default_logs_dir = None

    setup_logging(
        log_file or _default_log_file(paths, command_name=command_name, logs_dir=default_logs_dir)
    )
    if debug:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        for handler in root_logger.handlers:
            handler.setLevel(logging.DEBUG)
        logging.getLogger("urllib3").setLevel(logging.DEBUG)

    argv = " ".join(shlex.quote(a) for a in sys.argv)
    logging.info(f"Invocation: {argv}")


def _normalize_catalog(
    input_csv: Path, output_csv: Path, *, include_diagnostics: bool = True
) -> Path:
    df = read_csv(input_csv)
    if "Name" not in df.columns:
        raise SystemExit(f"Missing required column 'Name' in {input_csv}")

    # Preserve RowId (and pinned provider identifiers) when re-importing from a user export that
    # doesn't include RowId yet.
    if (
        output_csv.exists()
        and input_csv.resolve() != output_csv.resolve()
        and "RowId" not in df.columns
    ):
        prev = read_csv(output_csv)
        if "Name" in prev.columns and "RowId" in prev.columns:
            df["__occ"] = df.groupby("Name").cumcount()
            prev["__occ"] = prev.groupby("Name").cumcount()

            carry_cols = [
                c
                for c in (
                    "RowId",
                    "Disabled",
                    "YearHint",
                    "RAWG_ID",
                    "IGDB_ID",
                    "Steam_AppID",
                    "HLTB_ID",
                    "HLTB_Query",
                )
                if c in prev.columns
            ]
            if carry_cols:
                merged = df.merge(
                    prev[["Name", "__occ"] + carry_cols],
                    on=["Name", "__occ"],
                    how="left",
                    suffixes=("", "_prev"),
                )
                merged = merged.drop(columns=["__occ"])
                for c in carry_cols:
                    prev_col = f"{c}_prev"
                    if prev_col not in merged.columns:
                        continue
                    if c not in merged.columns:
                        merged[c] = merged[prev_col]
                    else:
                        mask = merged[c].astype(str).str.strip().eq("")
                        merged.loc[mask, c] = merged.loc[mask, prev_col]
                    merged = merged.drop(columns=[prev_col])
                df = merged

    if "RowId" in df.columns:
        rowid = df["RowId"].astype(str).str.strip()
        if rowid.duplicated().any():
            raise SystemExit(f"Duplicate RowId values in {input_csv}; fix them before importing.")

    required_cols: dict[str, str] = {
        "RowId": "",
        "Name": "",
        "Disabled": "",
        "YearHint": "",
        "RAWG_ID": "",
        "IGDB_ID": "",
        "Steam_AppID": "",
        "HLTB_ID": "",
        "HLTB_Query": "",
    }
    if include_diagnostics:
        required_cols.update({c: "" for c in DIAGNOSTIC_COLUMNS})

    df = ensure_columns(df, required_cols)
    df["Name"] = df["Name"].astype(str).str.strip()
    with_ids, created = ensure_row_ids(df)
    write_csv(with_ids, output_csv)
    logging.info(f"✔ Catalog normalized: {output_csv} (new ids: {created})")
    return output_csv


def _sync_back_catalog(
    *,
    catalog_csv: Path,
    enriched_csv: Path,
    output_csv: Path,
    deleted_mode: str = "disable",
) -> Path:
    catalog = read_csv(catalog_csv)
    enriched = read_csv(enriched_csv)
    if "RowId" not in enriched.columns:
        enriched = ensure_columns(enriched, ["RowId"])
    enriched, created = ensure_row_ids(enriched)
    if created:
        logging.info(f"ℹ sync: generated RowIds for {created} new enriched rows")

    if "RowId" not in catalog.columns:
        raise SystemExit(f"Catalog is missing RowId column: {catalog_csv}")

    catalog["RowId"] = catalog["RowId"].astype(str).str.strip()
    enriched["RowId"] = enriched["RowId"].astype(str).str.strip()

    provider_prefixes = ("RAWG_", "IGDB_", "Steam_", "SteamSpy_", "HLTB_")
    provider_id_cols = {"RAWG_ID", "IGDB_ID", "Steam_AppID", "HLTB_ID", "HLTB_Query"}
    always_keep = {"RowId", "Name"} | provider_id_cols

    sync_cols: list[str] = []
    for c in enriched.columns:
        if c in always_keep:
            sync_cols.append(c)
            continue
        if c.startswith(provider_prefixes):
            continue
        if c.startswith("__"):
            continue
        sync_cols.append(c)

    e_idx = enriched.set_index("RowId", drop=False)
    c_idx = catalog.set_index("RowId", drop=False)

    missing_in_enriched = [rid for rid in c_idx.index.tolist() if rid not in e_idx.index]
    if missing_in_enriched:
        if deleted_mode == "disable":
            if "Disabled" not in c_idx.columns:
                c_idx["Disabled"] = ""
            c_idx.loc[missing_in_enriched, "Disabled"] = "YES"
        elif deleted_mode == "drop":
            c_idx = c_idx.drop(index=missing_in_enriched)
        else:
            raise ValueError(f"Unknown deleted_mode: {deleted_mode}")

    for col in sync_cols:
        if col == "RowId":
            continue
        if col not in c_idx.columns:
            c_idx[col] = ""
        values = e_idx[col] if col in e_idx.columns else pd.Series([], dtype=object)
        common = c_idx.index.intersection(e_idx.index)
        c_idx.loc[common, col] = values.loc[common].values

    added = [rid for rid in e_idx.index.tolist() if rid not in c_idx.index]
    if added:
        add_rows = e_idx.loc[added].copy()
        # Only carry over sync columns for new rows; never introduce provider-derived columns.
        add_out = pd.DataFrame(index=add_rows.index)
        for col in c_idx.columns:
            add_out[col] = ""
        for col in sync_cols:
            if col == "RowId":
                continue
            if col not in add_rows.columns:
                continue
            if col not in add_out.columns:
                add_out[col] = ""
            add_out[col] = add_rows[col].values
        add_out["RowId"] = add_rows["RowId"].values
        c_idx = pd.concat([c_idx, add_out[c_idx.columns]], axis=0)

    out = c_idx.reset_index(drop=True)
    out = drop_eval_columns(out)
    write_csv(out, output_csv)
    logging.info(
        f"✔ sync updated catalog: {output_csv} (synced_cols={len(sync_cols)}, "
        f"added={len(added)}, deleted={len(missing_in_enriched)})"
    )
    return output_csv


def _legacy_enrich(argv: list[str]) -> None:
    """Backward-compatible mode: `run.py <input.csv> [options]`."""
    parser = argparse.ArgumentParser(
        description="Enrich video game catalogs with metadata from multiple APIs"
    )
    parser.add_argument(
        "input",
        type=Path,
        help="Input CSV file with game catalog",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output directory for generated files (default: data/output)",
    )
    parser.add_argument(
        "--cache",
        type=Path,
        help="Cache directory for API responses (default: data/cache)",
    )
    parser.add_argument(
        "--credentials",
        type=Path,
        help="Path to credentials.yaml file (default: data/credentials.yaml in project root)",
    )
    parser.add_argument(
        "--source",
        choices=["igdb", "rawg", "steam", "steamspy", "hltb", "all"],
        default="all",
        help="Which API source to process (default: all)",
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Merge all processed files into a final CSV",
    )
    parser.add_argument(
        "--merge-output",
        type=Path,
        help="Output file for merged results (default: data/output/Games_Enriched.csv)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG logging (default: INFO)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Generate a cross-provider validation report (default: off)",
    )
    parser.add_argument(
        "--validate-output",
        type=Path,
        help="Output file for validation report (default: data/output/Validation_Report.csv)",
    )

    args = parser.parse_args(argv)

    # Determine project root (parent of game_catalog_builder package)
    project_root, paths = _common_paths()

    # Set up logging (after paths are ensured)
    _setup_logging_from_args(
        paths, args.log_file, args.debug, command_name="legacy", input_path=args.input
    )
    logging.info("Starting game catalog enrichment")

    # Set up paths
    input_csv = args.input
    if not input_csv.exists():
        parser.error(f"Input file not found: {input_csv}")
    is_experiment = _is_under_dir(input_csv, paths.data_experiments)
    before = read_csv(input_csv)
    with_ids, created = ensure_row_ids(before)
    # If we had to create RowIds, avoid overwriting the user's original file: write a new input
    # next to the identity map (under the output dir) and use it from now on.
    if created > 0 or "RowId" not in before.columns:
        default_out_dir = (
            (paths.data_experiments / "output") if is_experiment else paths.data_output
        )
        safe_input = (
            args.output or default_out_dir
        ) / f"{input_csv.stem}_with_rowid{input_csv.suffix}"
        write_csv(with_ids, safe_input)
        logging.info(f"✔ RowId initialized: wrote new input CSV: {safe_input} (new ids: {created})")
        logging.info(f"ℹ Use this input file going forward: {safe_input}")
        input_csv = safe_input

    output_dir = args.output or (
        (paths.data_experiments / "output") if is_experiment else paths.data_output
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = args.cache or (
        (paths.data_experiments / "cache") if is_experiment else paths.data_cache
    )
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Read pinned IDs/queries directly from the input CSV when present.
    identity_overrides = load_identity_overrides(input_csv)

    # Load credentials
    if args.credentials:
        credentials_path = args.credentials
    else:
        # Default: look for data/credentials.yaml under project root
        credentials_path = project_root / "data" / "credentials.yaml"

    credentials = load_credentials(credentials_path)

    # Process based on source
    sources_to_process = (
        ["igdb", "rawg", "steam", "steamspy", "hltb"] if args.source == "all" else [args.source]
    )

    def run_source(source: str) -> None:
        if source == "igdb":
            process_igdb(
                input_csv=input_csv,
                output_csv=output_dir / "Provider_IGDB.csv",
                cache_path=cache_dir / "igdb_cache.json",
                credentials=credentials,
                required_cols=["IGDB_Name"],
                identity_overrides=identity_overrides or None,
            )
            return

        if source == "rawg":
            process_rawg(
                input_csv=input_csv,
                output_csv=output_dir / "Provider_RAWG.csv",
                cache_path=cache_dir / "rawg_cache.json",
                credentials=credentials,
                required_cols=["RAWG_ID", "RAWG_Year", "RAWG_Genre"],
                identity_overrides=identity_overrides or None,
            )
            return

        if source == "steam":
            process_steam(
                input_csv=input_csv,
                output_csv=output_dir / "Provider_Steam.csv",
                cache_path=cache_dir / "steam_cache.json",
                required_cols=["Steam_Name"],
                identity_overrides=identity_overrides or None,
            )
            return

        if source == "steamspy":
            process_steamspy(
                input_csv=output_dir / "Provider_Steam.csv",
                output_csv=output_dir / "Provider_SteamSpy.csv",
                cache_path=cache_dir / "steamspy_cache.json",
                required_cols=["SteamSpy_Owners"],
            )
            return

        if source == "steam+steamspy":
            process_steam_and_steamspy_streaming(
                input_csv=input_csv,
                steam_output_csv=output_dir / "Provider_Steam.csv",
                steamspy_output_csv=output_dir / "Provider_SteamSpy.csv",
                steam_cache_path=cache_dir / "steam_cache.json",
                steamspy_cache_path=cache_dir / "steamspy_cache.json",
                identity_overrides=identity_overrides or None,
            )
            return

        if source == "hltb":
            process_hltb(
                input_csv=input_csv,
                output_csv=output_dir / "Provider_HLTB.csv",
                cache_path=cache_dir / "hltb_cache.json",
                required_cols=["HLTB_Main"],
                identity_overrides=identity_overrides or None,
            )
            return

        raise ValueError(f"Unknown source: {source}")

    # Run providers in parallel when we have multiple independent sources.
    # SteamSpy can stream from discovered Steam appids; run via a combined pipeline when both are
    # requested.
    if len(sources_to_process) <= 1:
        run_source(sources_to_process[0])
    else:
        sources = list(sources_to_process)
        if "steam" in sources and "steamspy" in sources:
            sources = [s for s in sources if s not in ("steam", "steamspy")] + ["steam+steamspy"]

        parallel_sources = sources
        max_workers = min(len(parallel_sources), (os.cpu_count() or 4))

        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for src in parallel_sources:
                futures[executor.submit(run_source, src)] = src

            errors: list[tuple[str, BaseException]] = []
            for future in as_completed(futures):
                src = futures[future]
                try:
                    future.result()
                except BaseException as e:
                    errors.append((src, e))

            if errors:
                src, err = errors[0]
                raise RuntimeError(f"Provider '{src}' failed") from err

    # Merge if requested
    if args.merge or args.source == "all":
        merge_output = args.merge_output or (output_dir / "Games_Enriched.csv")
        merge_all(
            personal_csv=input_csv,
            rawg_csv=output_dir / "Provider_RAWG.csv",
            hltb_csv=output_dir / "Provider_HLTB.csv",
            steam_csv=output_dir / "Provider_Steam.csv",
            steamspy_csv=output_dir / "Provider_SteamSpy.csv",
            output_csv=merge_output,
            igdb_csv=output_dir / "Provider_IGDB.csv",
        )
        logging.info(f"✔ Games_Enriched.csv generated successfully: {merge_output}")

        if args.validate:
            validate_out = args.validate_output or (output_dir / "Validation_Report.csv")
            merged = read_csv(merge_output)
            report = generate_validation_report(merged)
            write_csv(report, validate_out)

            issues = report[report.get("ValidationTags", "").astype(str).str.strip().ne("")]

            # Coverage stats from merged output (how many rows have data per provider).
            def _count_non_empty(col: str) -> int:
                if col not in merged.columns:
                    return 0
                return int(merged[col].astype(str).str.strip().ne("").sum())

            def _count_any_prefix(prefix: str) -> int:
                cols = [c for c in merged.columns if c.startswith(prefix)]
                if not cols:
                    return 0
                any_non_empty = (
                    merged[cols]
                    .astype(str)
                    .apply(lambda s: s.str.strip().ne(""), axis=0)
                    .any(axis=1)
                )
                return int(any_non_empty.sum())

            total_rows = len(merged)
            logging.info(
                "✔ Provider coverage: "
                f"RAWG={_count_non_empty('RAWG_ID')}/{total_rows}, "
                f"IGDB={_count_non_empty('IGDB_ID')}/{total_rows}, "
                f"Steam={_count_non_empty('Steam_AppID')}/{total_rows}, "
                f"SteamSpy={_count_any_prefix('SteamSpy_')}/{total_rows}, "
                f"HLTB={_count_non_empty('HLTB_Main')}/{total_rows}"
            )

            logging.info(
                f"✔ Validation report generated: {validate_out} "
                f"(rows with issues: {len(issues)}/{len(report)})"
            )
            # Tag breakdown (compact) using ValidationTags.
            tags_counter: dict[str, int] = {}
            for t in report.get("ValidationTags", []).tolist():
                for part in str(t or "").split(","):
                    tag = part.strip()
                    if not tag:
                        continue
                    key = tag.split(":", 1)[0].strip()
                    tags_counter[key] = tags_counter.get(key, 0) + 1
            top = sorted(tags_counter.items(), key=lambda kv: kv[1], reverse=True)[
                : MATCHING.suggestions_limit
            ]
            if top:
                logging.info("✔ Validation top tags: " + ", ".join(f"{k}={v}" for k, v in top))

            for _, row in issues.head(20).iterrows():
                logging.warning(
                    f"[VALIDATE] {row.get('Name', '')}: "
                    f"Tags={row.get('ValidationTags', '') or ''}, "
                    f"Missing={row.get('MissingProviders', '') or ''}, "
                    f"Culprit={row.get('SuggestedCulprit', '') or ''}, "
                    f"Canonical={row.get('SuggestedCanonicalTitle', '') or ''} "
                    f"({row.get('SuggestedCanonicalSource', '') or ''})"
                )


def _command_normalize(args: argparse.Namespace) -> None:
    project_root, paths = _common_paths()
    _setup_logging_from_args(
        paths, args.log_file, args.debug, command_name="import", input_path=args.input
    )
    is_experiment = _is_under_dir(args.input, paths.data_experiments)
    cache_dir = args.cache or (
        (paths.data_experiments / "cache") if is_experiment else paths.data_cache
    )
    cache_dir.mkdir(parents=True, exist_ok=True)

    if args.out:
        out = args.out
    else:
        out = (
            (paths.data_experiments / "output" / f"{args.input.stem}_Catalog.csv")
            if is_experiment
            else (paths.data_input / "Games_Catalog.csv")
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    include_diagnostics = bool(args.diagnostics)
    _normalize_catalog(args.input, out, include_diagnostics=include_diagnostics)

    # Provider matching to populate pinned IDs and diagnostics.
    credentials_path = args.credentials or (project_root / "data" / "credentials.yaml")
    credentials = load_credentials(credentials_path)

    df = read_csv(out)
    sources = _parse_sources(
        args.source,
        allowed={"igdb", "rawg", "steam", "hltb"},
        aliases={"core": ["igdb", "rawg", "steam"]},
    )
    diag_clients: dict[str, object] = {}

    def _year_hint(row: pd.Series) -> int | None:
        for col in ("YearHint", "Year", "ReleaseYear", "Release_Year"):
            if col not in row.index:
                continue
            v = str(row.get(col, "") or "").strip()
            if v.isdigit() and len(v) == 4:
                y = int(v)
                if 1900 <= y <= 2100:
                    return y
        return extract_year_hint(str(row.get("Name", "") or ""))

    if "YearHint" in df.columns:
        for idx, row in df.iterrows():
            if _is_yes(row.get("Disabled", "")):
                continue
            existing = str(row.get("YearHint", "") or "").strip()
            if existing:
                continue
            inferred = extract_year_hint(str(row.get("Name", "") or ""))
            if inferred is not None:
                df.at[idx, "YearHint"] = str(inferred)

    if "rawg" in sources:
        api_key = credentials.get("rawg", {}).get("api_key", "")
        if api_key:
            client = RAWGClient(
                api_key=api_key,
                cache_path=cache_dir / "rawg_cache.json",
                min_interval_s=RAWG.min_interval_s,
            )
            diag_clients["rawg"] = client
            for idx, row in df.iterrows():
                if _is_yes(row.get("Disabled", "")):
                    continue
                name = str(row.get("Name", "") or "").strip()
                if not name:
                    continue
                rawg_id = str(row.get("RAWG_ID", "") or "").strip()
                if rawg_id == IDENTITY_NOT_FOUND:
                    if include_diagnostics:
                        df.at[idx, "RAWG_MatchedName"] = ""
                        df.at[idx, "RAWG_MatchScore"] = ""
                        df.at[idx, "RAWG_MatchedYear"] = ""
                    continue
                if rawg_id:
                    if not include_diagnostics:
                        continue
                    obj = client.get_by_id(rawg_id)
                else:
                    obj = client.search(name, year_hint=_year_hint(row))
                    if obj and obj.get("id") is not None:
                        df.at[idx, "RAWG_ID"] = str(obj.get("id") or "").strip()
                if include_diagnostics and obj and isinstance(obj, dict):
                    matched = str(obj.get("name") or "").strip()
                    released = str(obj.get("released") or "").strip()
                    df.at[idx, "RAWG_MatchedName"] = matched
                    df.at[idx, "RAWG_MatchScore"] = (
                        str(fuzzy_score(name, matched)) if matched else ""
                    )
                    df.at[idx, "RAWG_MatchedYear"] = released[:4] if len(released) >= 4 else ""

    if "igdb" in sources:
        client_id = credentials.get("igdb", {}).get("client_id", "")
        secret = credentials.get("igdb", {}).get("client_secret", "")
        if client_id and secret:
            client = IGDBClient(
                client_id=client_id,
                client_secret=secret,
                cache_path=cache_dir / "igdb_cache.json",
                min_interval_s=IGDB.min_interval_s,
            )
            diag_clients["igdb"] = client
            for idx, row in df.iterrows():
                if _is_yes(row.get("Disabled", "")):
                    continue
                name = str(row.get("Name", "") or "").strip()
                if not name:
                    continue
                igdb_id = str(row.get("IGDB_ID", "") or "").strip()
                if igdb_id == IDENTITY_NOT_FOUND:
                    if include_diagnostics:
                        df.at[idx, "IGDB_MatchedName"] = ""
                        df.at[idx, "IGDB_MatchScore"] = ""
                        df.at[idx, "IGDB_MatchedYear"] = ""
                    continue
                if igdb_id:
                    if not include_diagnostics:
                        continue
                    obj = client.get_by_id(igdb_id)
                else:
                    obj = client.search(name, year_hint=_year_hint(row))
                    if obj and str(obj.get("IGDB_ID", "") or "").strip():
                        df.at[idx, "IGDB_ID"] = str(obj.get("IGDB_ID") or "").strip()
                if include_diagnostics and obj and isinstance(obj, dict):
                    matched = str(obj.get("IGDB_Name") or "").strip()
                    df.at[idx, "IGDB_MatchedName"] = matched
                    df.at[idx, "IGDB_MatchScore"] = (
                        str(fuzzy_score(name, matched)) if matched else ""
                    )
                    df.at[idx, "IGDB_MatchedYear"] = str(obj.get("IGDB_Year") or "").strip()

    if "steam" in sources:
        client = SteamClient(
            cache_path=cache_dir / "steam_cache.json",
            min_interval_s=STEAM.storesearch_min_interval_s,
        )
        diag_clients["steam"] = client
        for idx, row in df.iterrows():
            if _is_yes(row.get("Disabled", "")):
                continue
            name = str(row.get("Name", "") or "").strip()
            if not name:
                continue
            steam_id = str(row.get("Steam_AppID", "") or "").strip()
            if not _platform_is_pc_like(row.get("Platform", "")) and not steam_id:
                # Don't auto-pin Steam AppIDs for clearly non-PC rows.
                if include_diagnostics:
                    df.at[idx, "Steam_MatchedName"] = ""
                    df.at[idx, "Steam_MatchScore"] = ""
                    df.at[idx, "Steam_MatchedYear"] = ""
                continue
            if steam_id == IDENTITY_NOT_FOUND:
                if include_diagnostics:
                    df.at[idx, "Steam_MatchedName"] = ""
                    df.at[idx, "Steam_MatchScore"] = ""
                    df.at[idx, "Steam_MatchedYear"] = ""
                continue
            matched = ""
            matched_year = ""
            if steam_id:
                if not include_diagnostics:
                    continue
                try:
                    details = client.get_app_details(int(steam_id))
                except ValueError:
                    details = None
                details_type = str((details or {}).get("type") or "").strip().lower()
                if not details or (details_type and details_type != "game"):
                    logging.warning(
                        f"[STEAM] Ignoring pinned Steam_AppID for '{name}': appid={steam_id} "
                        f"type={details_type or 'unknown'}"
                    )
                    df.at[idx, "Steam_AppID"] = ""
                    steam_id = ""
                else:
                    matched = str((details or {}).get("name") or "").strip()
                    release = (details or {}).get("release_date", {}) or {}
                    m = re.search(r"\b(19\d{2}|20\d{2})\b", str(release.get("date", "") or ""))
                    matched_year = m.group(1) if m else ""
            if not steam_id:
                # Cross-provider hints: try to infer Steam AppID from already-pinned providers.
                inferred = ""
                igdb_id = str(row.get("IGDB_ID", "") or "").strip()
                if igdb_id and igdb_id != IDENTITY_NOT_FOUND and diag_clients.get("igdb"):
                    igdb_obj = diag_clients["igdb"].get_by_id(igdb_id)
                    inferred = str((igdb_obj or {}).get("IGDB_SteamAppID") or "").strip()
                if not inferred:
                    rawg_id = str(row.get("RAWG_ID", "") or "").strip()
                    if rawg_id and rawg_id != IDENTITY_NOT_FOUND and diag_clients.get("rawg"):
                        rawg_obj = diag_clients["rawg"].get_by_id(rawg_id)
                        inferred = _extract_steam_appid_from_rawg(rawg_obj)
                inferred_ids = [
                    s.strip()
                    for s in re.split(r"[,\s]+", inferred)
                    if s.strip() and s.strip().isdigit()
                ]
                if inferred_ids:
                    inferred_int = int(inferred_ids[0])
                    details = client.get_app_details(inferred_int)
                    details_type = str((details or {}).get("type") or "").strip().lower()
                    if not details or (details_type and details_type != "game"):
                        logging.warning(
                            f"[STEAM] Ignoring inferred Steam AppID for '{name}': "
                            f"appid={inferred_ids[0]} type={details_type or 'unknown'}"
                        )
                    else:
                        df.at[idx, "Steam_AppID"] = str(inferred_int)
                        steam_id = str(inferred_int)
                        if include_diagnostics:
                            matched = str((details or {}).get("name") or "").strip()
                            df.at[idx, "Steam_MatchedName"] = matched
                            df.at[idx, "Steam_MatchScore"] = (
                                str(fuzzy_score(name, matched)) if matched else ""
                            )
                            release = (details or {}).get("release_date", {}) or {}
                            m = re.search(
                                r"\b(19\d{2}|20\d{2})\b", str(release.get("date", "") or "")
                            )
                            df.at[idx, "Steam_MatchedYear"] = m.group(1) if m else ""
                        # If we successfully inferred a Steam AppID, do not overwrite it by running
                        # a secondary name-based search (which can surface DLC/soundtrack matches).
                        continue

                search = client.search_appid(name, year_hint=_year_hint(row))
                if search and search.get("id") is not None:
                    appid_str = str(search.get("id") or "").strip()
                    df.at[idx, "Steam_AppID"] = appid_str

                    if include_diagnostics and appid_str.isdigit():
                        details = client.get_app_details(int(appid_str))
                        details_type = str((details or {}).get("type") or "").strip().lower()
                        if not details or (details_type and details_type != "game"):
                            logging.warning(
                                f"[STEAM] Ignoring Steam search result for '{name}': "
                                f"appid={appid_str} type={details_type or 'unknown'}"
                            )
                            df.at[idx, "Steam_AppID"] = ""
                            matched = ""
                            matched_year = ""
                        else:
                            matched = str((details or {}).get("name") or "").strip()
                            release = (details or {}).get("release_date", {}) or {}
                            m = re.search(
                                r"\b(19\d{2}|20\d{2})\b", str(release.get("date", "") or "")
                            )
                            matched_year = m.group(1) if m else ""
                    else:
                        matched = str((search or {}).get("name") or "").strip()
                else:
                    matched = ""
                    matched_year = ""
            if include_diagnostics:
                df.at[idx, "Steam_MatchedName"] = matched
                df.at[idx, "Steam_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""
                df.at[idx, "Steam_MatchedYear"] = matched_year

    if "hltb" in sources:
        client = HLTBClient(cache_path=cache_dir / "hltb_cache.json")
        diag_clients["hltb"] = client
        processed = 0
        for idx, row in df.iterrows():
            if _is_yes(row.get("Disabled", "")):
                continue
            name = str(row.get("Name", "") or "").strip()
            if not name:
                continue
            hltb_id = str(row.get("HLTB_ID", "") or "").strip()
            query = str(row.get("HLTB_Query", "") or "").strip()
            if hltb_id == IDENTITY_NOT_FOUND or query == IDENTITY_NOT_FOUND:
                if include_diagnostics:
                    df.at[idx, "HLTB_MatchedName"] = ""
                    df.at[idx, "HLTB_MatchScore"] = ""
                    df.at[idx, "HLTB_MatchedYear"] = ""
                    df.at[idx, "HLTB_MatchedPlatforms"] = ""
                continue

            if hltb_id and not include_diagnostics:
                # Pinned id is already present and we don't need to refresh match diagnostics.
                continue

            q = query or name
            data = client.search(name, query=q, hltb_id=hltb_id or None)
            if data and str(data.get("HLTB_ID", "") or "").strip() and not hltb_id:
                df.at[idx, "HLTB_ID"] = str(data.get("HLTB_ID") or "").strip()
            if include_diagnostics:
                matched = str((data or {}).get("HLTB_Name") or "").strip()
                df.at[idx, "HLTB_MatchedName"] = matched
                df.at[idx, "HLTB_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""
                df.at[idx, "HLTB_MatchedYear"] = str(
                    (data or {}).get("HLTB_ReleaseYear") or ""
                ).strip()
                df.at[idx, "HLTB_MatchedPlatforms"] = str(
                    (data or {}).get("HLTB_Platforms") or ""
                ).strip()

            processed += 1
            if processed % 25 == 0:
                # Persist incremental progress; HLTB can be slow and long runs should be resumable.
                write_csv(df, out)

    if include_diagnostics:
        df = fill_eval_tags(df, sources=set(sources), clients=diag_clients)
    else:
        df = drop_eval_columns(df)

    # Cache stats are logged at the end so they include any fetches performed during diagnostics
    # (e.g. Steam appdetails needed for year/series/platform checks).
    if "rawg" in diag_clients:
        logging.info(f"[RAWG] Cache stats: {diag_clients['rawg'].format_cache_stats()}")
    if "igdb" in diag_clients:
        logging.info(f"[IGDB] Cache stats: {diag_clients['igdb'].format_cache_stats()}")
    if "steam" in diag_clients:
        logging.info(f"[STEAM] Cache stats: {diag_clients['steam'].format_cache_stats()}")
    if "hltb" in diag_clients:
        logging.info(f"[HLTB] Cache stats: {diag_clients['hltb'].format_cache_stats()}")

    write_csv(df, out)
    logging.info(f"✔ Import matching completed: {out}")


def _command_enrich(args: argparse.Namespace) -> None:
    project_root, paths = _common_paths()
    _setup_logging_from_args(
        paths, args.log_file, args.debug, command_name="enrich", input_path=args.input
    )
    logging.info("Starting game catalog enrichment")

    is_experiment = _is_under_dir(args.input, paths.data_experiments)
    input_csv = args.input
    if not input_csv.exists():
        raise SystemExit(f"Input file not found: {input_csv}")

    output_dir = args.output or (
        (paths.data_experiments / "output") if is_experiment else paths.data_output
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = args.cache or (
        (paths.data_experiments / "cache") if is_experiment else paths.data_cache
    )
    cache_dir.mkdir(parents=True, exist_ok=True)

    merge_output = args.merge_output or (output_dir / "Games_Enriched.csv")
    in_place = input_csv.resolve() == merge_output.resolve()
    temp_base_csv: Path | None = None

    # For in-place enrich (input == merged output), strip provider columns before processing to
    # ensure derived/public columns are overwritten, not preserved.
    if in_place:
        base_df = build_personal_base_for_enrich(read_csv(input_csv))
        temp_base_csv = output_dir / f".personal_base.{os.getpid()}.csv"
        write_csv(base_df, temp_base_csv)
        input_for_processing = temp_base_csv
    else:
        input_for_processing = input_csv

    # Read pinned IDs/queries directly from the catalog.
    identity_overrides = load_identity_overrides(input_for_processing)

    # Load credentials
    if args.credentials:
        credentials_path = args.credentials
    else:
        credentials_path = project_root / "data" / "credentials.yaml"
    credentials = load_credentials(credentials_path)

    if args.clean_output:
        # Overwrite derived/public outputs for this command.
        for p in (
            output_dir / "Provider_IGDB.csv",
            output_dir / "Provider_RAWG.csv",
            output_dir / "Provider_Steam.csv",
            output_dir / "Provider_SteamSpy.csv",
            output_dir / "Provider_HLTB.csv",
            output_dir / "Games_Enriched.csv",
            output_dir / "Validation_Report.csv",
        ):
            if p.exists():
                p.unlink()

    sources_to_process = _parse_sources(
        args.source,
        allowed={"igdb", "rawg", "steam", "steamspy", "hltb"},
        aliases={"core": ["igdb", "rawg", "steam"]},
    )

    def run_source(source: str) -> None:
        if source == "igdb":
            process_igdb(
                input_csv=input_csv,
                output_csv=output_dir / "Provider_IGDB.csv",
                cache_path=cache_dir / "igdb_cache.json",
                credentials=credentials,
                required_cols=["IGDB_Name"],
                identity_overrides=identity_overrides or None,
            )
            return

        if source == "rawg":
            process_rawg(
                input_csv=input_csv,
                output_csv=output_dir / "Provider_RAWG.csv",
                cache_path=cache_dir / "rawg_cache.json",
                credentials=credentials,
                required_cols=["RAWG_ID", "RAWG_Year", "RAWG_Genre"],
                identity_overrides=identity_overrides or None,
            )
            return

        if source == "steam":
            process_steam(
                input_csv=input_csv,
                output_csv=output_dir / "Provider_Steam.csv",
                cache_path=cache_dir / "steam_cache.json",
                required_cols=["Steam_Name"],
                identity_overrides=identity_overrides or None,
            )
            return

        if source == "steamspy":
            process_steamspy(
                input_csv=output_dir / "Provider_Steam.csv",
                output_csv=output_dir / "Provider_SteamSpy.csv",
                cache_path=cache_dir / "steamspy_cache.json",
                required_cols=["SteamSpy_Owners"],
            )
            return

        if source == "steam+steamspy":
            process_steam_and_steamspy_streaming(
                input_csv=input_csv,
                steam_output_csv=output_dir / "Provider_Steam.csv",
                steamspy_output_csv=output_dir / "Provider_SteamSpy.csv",
                steam_cache_path=cache_dir / "steam_cache.json",
                steamspy_cache_path=cache_dir / "steamspy_cache.json",
                identity_overrides=identity_overrides or None,
            )
            return

        if source == "hltb":
            process_hltb(
                input_csv=input_csv,
                output_csv=output_dir / "Provider_HLTB.csv",
                cache_path=cache_dir / "hltb_cache.json",
                required_cols=["HLTB_Main"],
                identity_overrides=identity_overrides or None,
            )
            return

        raise ValueError(f"Unknown source: {source}")

    # Run providers in parallel when we have multiple independent sources.
    if len(sources_to_process) <= 1:
        run_source(sources_to_process[0])
    else:
        sources = list(sources_to_process)
        if "steam" in sources and "steamspy" in sources:
            sources = [s for s in sources if s not in ("steam", "steamspy")] + ["steam+steamspy"]

        max_workers = min(len(sources), (os.cpu_count() or 4))
        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for src in sources:
                futures[executor.submit(run_source, src)] = src

            errors: list[tuple[str, BaseException]] = []
            for future in as_completed(futures):
                src = futures[future]
                try:
                    future.result()
                except BaseException as e:
                    errors.append((src, e))
            if errors:
                src, err = errors[0]
                raise RuntimeError(f"Provider '{src}' failed") from err

    merge_all(
        personal_csv=input_for_processing,
        rawg_csv=output_dir / "Provider_RAWG.csv",
        hltb_csv=output_dir / "Provider_HLTB.csv",
        steam_csv=output_dir / "Provider_Steam.csv",
        steamspy_csv=output_dir / "Provider_SteamSpy.csv",
        output_csv=merge_output,
        igdb_csv=output_dir / "Provider_IGDB.csv",
    )
    merged_df = read_csv(merge_output)
    merged_df = drop_eval_columns(merged_df)
    write_csv(merged_df, merge_output)
    logging.info(f"✔ Games_Enriched.csv generated successfully: {merge_output}")

    if args.validate:
        validate_out = args.validate_output or (output_dir / "Validation_Report.csv")
        merged = read_csv(merge_output)
        enabled_for_validation = {s.strip().lower() for s in sources_to_process if s.strip()}
        report = generate_validation_report(merged, enabled_providers=enabled_for_validation)
        write_csv(report, validate_out)
        logging.info(f"✔ Validation report generated: {validate_out}")

    if temp_base_csv and temp_base_csv.exists():
        temp_base_csv.unlink()


def _command_sync_back(args: argparse.Namespace) -> None:
    _, paths = _common_paths()
    _setup_logging_from_args(
        paths, args.log_file, args.debug, command_name="sync", input_path=args.catalog
    )
    out = args.out or args.catalog
    _sync_back_catalog(catalog_csv=args.catalog, enriched_csv=args.enriched, output_csv=out)


def _command_validate(args: argparse.Namespace) -> None:
    _, paths = _common_paths()
    _setup_logging_from_args(
        paths,
        args.log_file,
        args.debug,
        command_name="validate",
        input_path=args.enriched,
    )
    is_experiment = _is_under_dir(args.enriched, paths.data_experiments)
    output_dir = args.output_dir or (
        (paths.data_experiments / "output") if is_experiment else paths.data_output
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    out = args.out or (output_dir / "Validation_Report.csv")
    if args.enriched:
        enriched = read_csv(args.enriched)
    else:
        enriched_path = output_dir / "Games_Enriched.csv"
        if not enriched_path.exists():
            raise SystemExit(
                f"Missing {enriched_path}; run `enrich` first or pass --enriched explicitly"
            )
        enriched = read_csv(enriched_path)
    report = generate_validation_report(enriched)
    write_csv(report, out)
    logging.info(f"✔ Validation report generated: {out}")


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in {"import", "enrich", "sync", "validate"}:
        parser = argparse.ArgumentParser(description="Enrich video game catalogs with metadata")
        sub = parser.add_subparsers(dest="command", required=True)

        p_import = sub.add_parser(
            "import", help="Normalize an exported user CSV into Games_Catalog.csv"
        )
        p_import.add_argument("input", type=Path, help="Input CSV (exported from spreadsheet)")
        p_import.add_argument(
            "--out",
            type=Path,
            help="Output catalog CSV (default: data/input/Games_Catalog.csv)",
        )
        p_import.add_argument(
            "--log-file",
            type=Path,
            help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
        )
        p_import.add_argument("--cache", type=Path, help="Cache directory (default: data/cache)")
        p_import.add_argument(
            "--credentials", type=Path, help="Credentials YAML (default: data/credentials.yaml)"
        )
        p_import.add_argument(
            "--source",
            type=str,
            default="all",
            help=(
                "Which providers to match for IDs (e.g. 'core' or 'igdb,rawg,steam') (default: all)"
            ),
        )
        p_import.add_argument(
            "--diagnostics",
            action=argparse.BooleanOptionalAction,
            default=True,
            help="Include match diagnostics columns in the output (default: true)",
        )
        p_import.add_argument(
            "--debug", action="store_true", help="Enable DEBUG logging (default: INFO)"
        )
        p_import.set_defaults(_fn=_command_normalize)

        p_enrich = sub.add_parser(
            "enrich", help="Generate provider outputs + Games_Enriched.csv from Games_Catalog.csv"
        )
        p_enrich.add_argument("input", type=Path, help="Catalog CSV (source of truth)")
        p_enrich.add_argument("--output", type=Path, help="Output directory (default: data/output)")
        p_enrich.add_argument("--cache", type=Path, help="Cache directory (default: data/cache)")
        p_enrich.add_argument(
            "--credentials", type=Path, help="Credentials YAML (default: data/credentials.yaml)"
        )
        p_enrich.add_argument(
            "--source",
            type=str,
            default="all",
        )
        p_enrich.add_argument(
            "--clean-output",
            action=argparse.BooleanOptionalAction,
            default=True,
            help="Delete and regenerate provider/output CSVs (default: true)",
        )
        p_enrich.add_argument(
            "--merge-output",
            type=Path,
            help="Output file for merged results (default: data/output/Games_Enriched.csv)",
        )
        p_enrich.add_argument(
            "--validate", action="store_true", help="Generate validation report (default: off)"
        )
        p_enrich.add_argument(
            "--validate-output",
            type=Path,
            help="Output file for validation report (default: data/output/Validation_Report.csv)",
        )
        p_enrich.add_argument(
            "--log-file",
            type=Path,
            help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
        )
        p_enrich.add_argument(
            "--debug", action="store_true", help="Enable DEBUG logging (default: INFO)"
        )
        p_enrich.set_defaults(_fn=_command_enrich)

        p_sync = sub.add_parser(
            "sync",
            help="Sync user-editable fields from Games_Enriched.csv back into Games_Catalog.csv",
        )
        p_sync.add_argument("catalog", type=Path, help="Catalog CSV to update")
        p_sync.add_argument("enriched", type=Path, help="Edited enriched CSV")
        p_sync.add_argument(
            "--out", type=Path, help="Output catalog CSV (default: overwrite catalog)"
        )
        p_sync.add_argument(
            "--log-file",
            type=Path,
            help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
        )
        p_sync.add_argument(
            "--debug", action="store_true", help="Enable DEBUG logging (default: INFO)"
        )
        p_sync.set_defaults(_fn=_command_sync_back)

        p_val = sub.add_parser(
            "validate", help="Generate validation report from an enriched CSV (read-only)"
        )
        p_val.add_argument(
            "--enriched",
            type=Path,
            help="Enriched CSV to validate (default: data/output/Games_Enriched.csv)",
        )
        p_val.add_argument(
            "--output-dir", type=Path, help="Output directory (default: data/output)"
        )
        p_val.add_argument(
            "--out",
            type=Path,
            help="Output validation report path (default: <output-dir>/Validation_Report.csv)",
        )
        p_val.add_argument(
            "--log-file",
            type=Path,
            help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
        )
        p_val.add_argument(
            "--debug", action="store_true", help="Enable DEBUG logging (default: INFO)"
        )
        p_val.set_defaults(_fn=_command_validate)

        ns = parser.parse_args(argv)
        ns._fn(ns)
        return

    _legacy_enrich(argv)


if __name__ == "__main__":
    main()
