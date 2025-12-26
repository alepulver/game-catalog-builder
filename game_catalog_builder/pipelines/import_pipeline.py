from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..diagnostics.import_diagnostics import fill_eval_tags, platform_is_pc_like
from ..pipelines.common import log_cache_stats
from ..pipelines.context import PipelineContext
from ..pipelines.protocols import (
    HLTBClientLike,
    IGDBClientLike,
    RAWGClientLike,
    SteamClientLike,
    WikidataClientLike,
)
from ..schema import DIAGNOSTIC_COLUMNS
from ..utils import (
    IDENTITY_NOT_FOUND,
    ensure_columns,
    ensure_row_ids,
    extract_year_hint,
    fuzzy_score,
    read_csv,
    write_csv,
)
from ..utils.periodic import EveryN
from ..utils.progress import Progress


def normalize_catalog(
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
                    "Wikidata_QID",
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
        "Wikidata_QID": "",
    }
    if include_diagnostics:
        required_cols.update({c: "" for c in DIAGNOSTIC_COLUMNS})

    df = ensure_columns(df, required_cols)
    df["Name"] = df["Name"].astype(str).str.strip()
    with_ids, created = ensure_row_ids(df)
    write_csv(with_ids, output_csv)
    logging.info(f"✔ Catalog normalized: {output_csv} (new ids: {created})")
    return output_csv


def _is_yes(v: object) -> bool:
    return str(v or "").strip().upper() in {"YES", "Y", "TRUE", "1"}


def _year_hint_from_row(row: pd.Series) -> int | None:
    for col in ("YearHint", "Year", "ReleaseYear", "Release_Year"):
        if col not in row.index:
            continue
        v = str(row.get(col, "") or "").strip()
        if v.isdigit() and len(v) == 4:
            y = int(v)
            if 1900 <= y <= 2100:
                return y
    return extract_year_hint(str(row.get("Name", "") or ""))


def _ensure_year_hint_column(df: pd.DataFrame) -> None:
    if "YearHint" not in df.columns:
        return
    for idx, row in df.iterrows():
        if _is_yes(row.get("Disabled", "")):
            continue
        existing = str(row.get("YearHint", "") or "").strip()
        if existing:
            continue
        inferred = extract_year_hint(str(row.get("Name", "") or ""))
        if inferred is not None:
            df.at[idx, "YearHint"] = str(inferred)


def _match_rawg_ids(
    df: pd.DataFrame,
    *,
    client: RAWGClientLike,
    include_diagnostics: bool,
    active_total: int,
) -> None:
    progress = Progress("RAWG", total=active_total, every_n=100)
    seen = 0
    for idx, row in df.iterrows():
        if _is_yes(row.get("Disabled", "")):
            continue
        name = str(row.get("Name", "") or "").strip()
        if not name:
            continue
        seen += 1

        rawg_id = str(row.get("RAWG_ID", "") or "").strip()
        if rawg_id == IDENTITY_NOT_FOUND:
            if include_diagnostics:
                df.at[idx, "RAWG_MatchedName"] = ""
                df.at[idx, "RAWG_MatchScore"] = ""
                df.at[idx, "RAWG_MatchedYear"] = ""
            progress.maybe_log(seen)
            continue

        if rawg_id:
            if not include_diagnostics:
                progress.maybe_log(seen)
                continue
            obj = client.get_by_id(rawg_id)
        else:
            obj = client.search(name, year_hint=_year_hint_from_row(row))
            if obj and obj.get("id") is not None:
                df.at[idx, "RAWG_ID"] = str(obj.get("id") or "").strip()

        if include_diagnostics and obj and isinstance(obj, dict):
            matched = str(obj.get("name") or "").strip()
            released = str(obj.get("released") or "").strip()
            df.at[idx, "RAWG_MatchedName"] = matched
            df.at[idx, "RAWG_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""
            df.at[idx, "RAWG_MatchedYear"] = released[:4] if len(released) >= 4 else ""

        progress.maybe_log(seen)


def _match_igdb_ids(
    df: pd.DataFrame,
    *,
    client: IGDBClientLike,
    include_diagnostics: bool,
    active_total: int,
) -> None:
    progress = Progress("IGDB", total=active_total, every_n=100)
    seen = 0
    for idx, row in df.iterrows():
        if _is_yes(row.get("Disabled", "")):
            continue
        name = str(row.get("Name", "") or "").strip()
        if not name:
            continue
        seen += 1

        igdb_id = str(row.get("IGDB_ID", "") or "").strip()
        if igdb_id == IDENTITY_NOT_FOUND:
            if include_diagnostics:
                df.at[idx, "IGDB_MatchedName"] = ""
                df.at[idx, "IGDB_MatchScore"] = ""
                df.at[idx, "IGDB_MatchedYear"] = ""
            progress.maybe_log(seen)
            continue

        if igdb_id:
            if not include_diagnostics:
                progress.maybe_log(seen)
                continue
            obj = client.get_by_id(igdb_id)
        else:
            obj = client.search(name, year_hint=_year_hint_from_row(row))
            if obj and str(obj.get("IGDB_ID", "") or "").strip():
                df.at[idx, "IGDB_ID"] = str(obj.get("IGDB_ID") or "").strip()

        if include_diagnostics and obj and isinstance(obj, dict):
            matched = str(obj.get("IGDB_Name") or "").strip()
            df.at[idx, "IGDB_MatchedName"] = matched
            df.at[idx, "IGDB_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""
            df.at[idx, "IGDB_MatchedYear"] = str(obj.get("IGDB_Year") or "").strip()

        progress.maybe_log(seen)


def _match_steam_appids(
    df: pd.DataFrame,
    *,
    steam: SteamClientLike,
    igdb: IGDBClientLike | None,
    include_diagnostics: bool,
    active_total: int,
) -> None:
    progress = Progress("STEAM", total=active_total, every_n=100)
    seen = 0

    def _details_is_game(d: object) -> bool:
        if not isinstance(d, dict):
            return False
        t = str(d.get("type") or "").strip().lower()
        return not t or t == "game"

    def _apply_details(row_idx: int, personal: str, d: dict) -> None:
        matched = str(d.get("name") or "").strip()
        date = str((d.get("release_date") or {}).get("date") or "").strip()
        if include_diagnostics:
            df.at[row_idx, "Steam_MatchedName"] = matched
            df.at[row_idx, "Steam_MatchScore"] = (
                str(fuzzy_score(personal, matched)) if matched else ""
            )
            df.at[row_idx, "Steam_MatchedYear"] = str(extract_year_hint(date) or "")
            if "Steam_StoreType" in df.columns:
                df.at[row_idx, "Steam_StoreType"] = str(d.get("type") or "").strip().lower()

    for idx, row in df.iterrows():
        if _is_yes(row.get("Disabled", "")):
            continue
        name = str(row.get("Name", "") or "").strip()
        if not name:
            continue
        seen += 1

        steam_id = str(row.get("Steam_AppID", "") or "").strip()
        if not platform_is_pc_like(row.get("Platform", "")) and not steam_id:
            if include_diagnostics:
                df.at[idx, "Steam_MatchedName"] = ""
                df.at[idx, "Steam_MatchScore"] = ""
                df.at[idx, "Steam_MatchedYear"] = ""
            progress.maybe_log(seen)
            continue

        if steam_id == IDENTITY_NOT_FOUND:
            if include_diagnostics:
                df.at[idx, "Steam_MatchedName"] = ""
                df.at[idx, "Steam_MatchScore"] = ""
                df.at[idx, "Steam_MatchedYear"] = ""
                if "Steam_RejectedReason" in df.columns:
                    df.at[idx, "Steam_RejectedReason"] = ""
            progress.maybe_log(seen)
            continue

        if steam_id and steam_id.isdigit():
            details = steam.get_app_details(int(steam_id))
            if not _details_is_game(details):
                df.at[idx, "Steam_AppID"] = ""
                steam_id = ""
            elif include_diagnostics and isinstance(details, dict):
                _apply_details(int(idx), name, details)
                progress.maybe_log(seen)
                continue

        if not steam_id and igdb is not None:
            igdb_id = str(row.get("IGDB_ID", "") or "").strip()
            if igdb_id and igdb_id != IDENTITY_NOT_FOUND:
                igdb_obj = igdb.get_by_id(igdb_id)
                inferred = str((igdb_obj or {}).get("IGDB_SteamAppID") or "").strip()
                if inferred.isdigit():
                    inferred_details = steam.get_app_details(int(inferred))
                    if _details_is_game(inferred_details) and isinstance(inferred_details, dict):
                        df.at[idx, "Steam_AppID"] = inferred
                        steam_id = inferred
                        _apply_details(int(idx), name, inferred_details)
                        progress.maybe_log(seen)
                        continue

        res = steam.search_appid(name, year_hint=_year_hint_from_row(row))
        matched = str((res or {}).get("name") or "").strip()
        if res and res.get("id") is not None:
            df.at[idx, "Steam_AppID"] = str(res.get("id") or "").strip()
        if include_diagnostics:
            df.at[idx, "Steam_MatchedName"] = matched
            df.at[idx, "Steam_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""
            df.at[idx, "Steam_MatchedYear"] = str((res or {}).get("release_year") or "").strip()
            if "Steam_RejectedReason" in df.columns:
                df.at[idx, "Steam_RejectedReason"] = str((res or {}).get("rejected_reason") or "").strip()
            if "Steam_StoreType" in df.columns:
                df.at[idx, "Steam_StoreType"] = str((res or {}).get("store_type") or "").strip()

        progress.maybe_log(seen)


def _match_hltb_ids(
    df: pd.DataFrame,
    *,
    client: HLTBClientLike,
    include_diagnostics: bool,
    active_total: int,
    output_csv: Path,
) -> None:
    progress = Progress("HLTB", total=active_total, every_n=25)
    seen = 0
    writer = EveryN(every_n=25, callback=lambda: write_csv(df, output_csv))

    for idx, row in df.iterrows():
        if _is_yes(row.get("Disabled", "")):
            continue
        name = str(row.get("Name", "") or "").strip()
        if not name:
            continue
        seen += 1
        hltb_id = str(row.get("HLTB_ID", "") or "").strip()
        hltb_query = str(row.get("HLTB_Query", "") or "").strip() or name
        if hltb_id == IDENTITY_NOT_FOUND or hltb_query == IDENTITY_NOT_FOUND:
            if include_diagnostics:
                df.at[idx, "HLTB_MatchedName"] = ""
                df.at[idx, "HLTB_MatchScore"] = ""
                df.at[idx, "HLTB_MatchedYear"] = ""
                df.at[idx, "HLTB_MatchedPlatforms"] = ""
            progress.maybe_log(seen)
            continue

        if hltb_id:
            if not include_diagnostics:
                progress.maybe_log(seen)
                continue
            obj = client.get_by_id(hltb_id)
            if obj and isinstance(obj, dict):
                matched = str(obj.get("HLTB_Name") or "").strip()
                df.at[idx, "HLTB_MatchedName"] = matched
                df.at[idx, "HLTB_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""
                df.at[idx, "HLTB_MatchedYear"] = str(obj.get("HLTB_ReleaseYear") or "").strip()
                df.at[idx, "HLTB_MatchedPlatforms"] = str(obj.get("HLTB_Platforms") or "").strip()
            progress.maybe_log(seen)
            writer.maybe(seen)
            continue

        res = client.search(name, query=hltb_query, hltb_id=None)
        if res and isinstance(res, dict):
            rid = str(res.get("HLTB_ID") or "").strip()
            if rid:
                df.at[idx, "HLTB_ID"] = rid
            df.at[idx, "HLTB_Query"] = hltb_query
            if include_diagnostics:
                matched = str(res.get("HLTB_Name") or "").strip()
                df.at[idx, "HLTB_MatchedName"] = matched
                df.at[idx, "HLTB_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""
                df.at[idx, "HLTB_MatchedYear"] = str(res.get("HLTB_ReleaseYear") or "").strip()
                df.at[idx, "HLTB_MatchedPlatforms"] = str(res.get("HLTB_Platforms") or "").strip()
        progress.maybe_log(seen)
        writer.maybe(seen)


def _match_wikidata_qids(
    df: pd.DataFrame,
    *,
    client: WikidataClientLike,
    include_diagnostics: bool,
    active_total: int,
) -> None:
    progress = Progress("WIKIDATA", total=active_total, every_n=100)
    seen = 0
    for idx, row in df.iterrows():
        if _is_yes(row.get("Disabled", "")):
            continue
        name = str(row.get("Name", "") or "").strip()
        if not name:
            continue
        seen += 1
        qid = str(row.get("Wikidata_QID", "") or "").strip()
        if qid == IDENTITY_NOT_FOUND:
            if include_diagnostics:
                df.at[idx, "Wikidata_MatchedLabel"] = ""
                df.at[idx, "Wikidata_MatchScore"] = ""
                df.at[idx, "Wikidata_MatchedYear"] = ""
            progress.maybe_log(seen)
            continue
        if qid:
            if not include_diagnostics:
                progress.maybe_log(seen)
                continue
            obj = client.get_by_id(qid)
        else:
            obj = client.search(name, year_hint=_year_hint_from_row(row))
            if obj and str(obj.get("Wikidata_QID", "") or "").strip():
                df.at[idx, "Wikidata_QID"] = str(obj.get("Wikidata_QID") or "").strip()
        if include_diagnostics and obj and isinstance(obj, dict):
            matched = str(obj.get("Wikidata_Label") or obj.get("Wikidata_MatchedLabel") or "").strip()
            df.at[idx, "Wikidata_MatchedLabel"] = matched
            df.at[idx, "Wikidata_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""
            df.at[idx, "Wikidata_MatchedYear"] = str(obj.get("Wikidata_ReleaseYear") or "").strip()
        progress.maybe_log(seen)


def run_import(
    ctx: PipelineContext,
    *,
    input_csv: Path,
    output_csv: Path,
    include_diagnostics: bool,
) -> Path:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    ctx.cache_dir.mkdir(parents=True, exist_ok=True)

    normalize_catalog(input_csv, output_csv, include_diagnostics=include_diagnostics)
    df = read_csv(output_csv)
    _ensure_year_hint_column(df)

    clients = ctx.build_clients()
    active_total = sum(1 for _, r in df.iterrows() if not _is_yes(r.get("Disabled", "")))

    rawg = clients.get("rawg")
    if "rawg" in ctx.sources and rawg is not None:
        _match_rawg_ids(df, client=rawg, include_diagnostics=include_diagnostics, active_total=active_total)  # type: ignore[arg-type]

    igdb = clients.get("igdb")
    if "igdb" in ctx.sources and igdb is not None:
        _match_igdb_ids(df, client=igdb, include_diagnostics=include_diagnostics, active_total=active_total)  # type: ignore[arg-type]

    steam = clients.get("steam")
    if "steam" in ctx.sources and steam is not None:
        _match_steam_appids(
            df,
            steam=steam,  # type: ignore[arg-type]
            igdb=igdb,  # type: ignore[arg-type]
            include_diagnostics=include_diagnostics,
            active_total=active_total,
        )

    hltb = clients.get("hltb")
    if "hltb" in ctx.sources and hltb is not None:
        _match_hltb_ids(
            df,
            client=hltb,  # type: ignore[arg-type]
            include_diagnostics=include_diagnostics,
            active_total=active_total,
            output_csv=output_csv,
        )

    wikidata = clients.get("wikidata")
    if "wikidata" in ctx.sources and wikidata is not None:
        _match_wikidata_qids(
            df,
            client=wikidata,  # type: ignore[arg-type]
            include_diagnostics=include_diagnostics,
            active_total=active_total,
        )

    if include_diagnostics:
        df = fill_eval_tags(df, sources=set(ctx.sources), clients=clients)
    else:
        for c in ("ReviewTags", "MatchConfidence"):
            if c in df.columns:
                df[c] = ""

    log_cache_stats(clients)

    write_csv(df, output_csv)
    logging.info(f"✔ Import matching completed: {output_csv}")
    return output_csv
