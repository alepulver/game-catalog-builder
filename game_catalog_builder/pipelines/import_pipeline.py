from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from ..analysis.import_diagnostics import fill_eval_tags, platform_is_pc_like
from ..config import CLI
from ..pipelines.common import iter_named_rows_with_progress, log_cache_stats, write_full_csv
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
)


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
    write_full_csv(with_ids, output_csv)
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
    for idx, row, name, _seen in iter_named_rows_with_progress(
        df,
        label="RAWG",
        total=active_total,
        skip_row=lambda r: _is_yes(r.get("Disabled", "")),
    ):
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
            obj = client.search(name, year_hint=_year_hint_from_row(row))
            if obj and obj.get("id") is not None:
                df.at[idx, "RAWG_ID"] = str(obj.get("id") or "").strip()

        if include_diagnostics and obj and isinstance(obj, dict):
            matched = str(obj.get("name") or "").strip()
            released = str(obj.get("released") or "").strip()
            df.at[idx, "RAWG_MatchedName"] = matched
            df.at[idx, "RAWG_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""
            df.at[idx, "RAWG_MatchedYear"] = released[:4] if len(released) >= 4 else ""


def _match_igdb_ids(
    df: pd.DataFrame,
    *,
    client: IGDBClientLike,
    include_diagnostics: bool,
    active_total: int,
) -> None:
    for idx, row, name, _seen in iter_named_rows_with_progress(
        df,
        label="IGDB",
        total=active_total,
        skip_row=lambda r: _is_yes(r.get("Disabled", "")),
    ):
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
            obj = client.search(name, year_hint=_year_hint_from_row(row))
            if obj and str(obj.get("IGDB_ID", "") or "").strip():
                df.at[idx, "IGDB_ID"] = str(obj.get("IGDB_ID") or "").strip()

        if include_diagnostics and obj and isinstance(obj, dict):
            matched = str(obj.get("IGDB_Name") or "").strip()
            df.at[idx, "IGDB_MatchedName"] = matched
            df.at[idx, "IGDB_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""
            df.at[idx, "IGDB_MatchedYear"] = str(obj.get("IGDB_Year") or "").strip()


def _match_steam_appids(
    df: pd.DataFrame,
    *,
    steam: SteamClientLike,
    igdb: IGDBClientLike | None,
    include_diagnostics: bool,
    active_total: int,
) -> None:
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

    for idx, row, name, _seen in iter_named_rows_with_progress(
        df,
        label="STEAM",
        total=active_total,
        skip_row=lambda r: _is_yes(r.get("Disabled", "")),
    ):
        steam_id = str(row.get("Steam_AppID", "") or "").strip()
        if not platform_is_pc_like(row.get("Platform", "")) and not steam_id:
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
                if "Steam_RejectedReason" in df.columns:
                    df.at[idx, "Steam_RejectedReason"] = ""
            continue

        if steam_id and steam_id.isdigit():
            details = steam.get_app_details(int(steam_id))
            if not _details_is_game(details):
                df.at[idx, "Steam_AppID"] = ""
                steam_id = ""
            elif include_diagnostics and isinstance(details, dict):
                _apply_details(int(idx), name, details)
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
                df.at[idx, "Steam_RejectedReason"] = str(
                    (res or {}).get("rejected_reason") or ""
                ).strip()
            if "Steam_StoreType" in df.columns:
                df.at[idx, "Steam_StoreType"] = str((res or {}).get("store_type") or "").strip()


def _match_hltb_ids(
    df: pd.DataFrame,
    *,
    client: HLTBClientLike,
    include_diagnostics: bool,
    active_total: int,
) -> None:
    for idx, row, name, _seen in iter_named_rows_with_progress(
        df,
        label="HLTB",
        total=active_total,
        skip_row=lambda r: _is_yes(r.get("Disabled", "")),
    ):
        hltb_id = str(row.get("HLTB_ID", "") or "").strip()
        hltb_query = str(row.get("HLTB_Query", "") or "").strip() or name
        if hltb_id == IDENTITY_NOT_FOUND or hltb_query == IDENTITY_NOT_FOUND:
            if include_diagnostics:
                df.at[idx, "HLTB_MatchedName"] = ""
                df.at[idx, "HLTB_MatchScore"] = ""
                df.at[idx, "HLTB_MatchedYear"] = ""
                df.at[idx, "HLTB_MatchedPlatforms"] = ""
            continue

        if hltb_id:
            if not include_diagnostics:
                continue
            obj = client.get_by_id(hltb_id)
            if obj and isinstance(obj, dict):
                matched = str(obj.get("HLTB_Name") or "").strip()
                df.at[idx, "HLTB_MatchedName"] = matched
                df.at[idx, "HLTB_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""
                df.at[idx, "HLTB_MatchedYear"] = str(obj.get("HLTB_ReleaseYear") or "").strip()
                df.at[idx, "HLTB_MatchedPlatforms"] = str(obj.get("HLTB_Platforms") or "").strip()
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


def _match_wikidata_qids(
    df: pd.DataFrame,
    *,
    client: WikidataClientLike,
    include_diagnostics: bool,
    active_total: int,
) -> None:
    for idx, row, name, _seen in iter_named_rows_with_progress(
        df,
        label="WIKIDATA",
        total=active_total,
        skip_row=lambda r: _is_yes(r.get("Disabled", "")),
    ):
        qid = str(row.get("Wikidata_QID", "") or "").strip()
        if qid == IDENTITY_NOT_FOUND:
            if include_diagnostics:
                df.at[idx, "Wikidata_MatchedLabel"] = ""
                df.at[idx, "Wikidata_MatchScore"] = ""
                df.at[idx, "Wikidata_MatchedYear"] = ""
            continue
        if qid:
            if not include_diagnostics:
                continue
            obj = client.get_by_id(qid)
        else:
            obj = client.search(name, year_hint=_year_hint_from_row(row))
            if obj and str(obj.get("Wikidata_QID", "") or "").strip():
                df.at[idx, "Wikidata_QID"] = str(obj.get("Wikidata_QID") or "").strip()
        if include_diagnostics and obj and isinstance(obj, dict):
            matched = str(
                obj.get("Wikidata_Label") or obj.get("Wikidata_MatchedLabel") or ""
            ).strip()
            df.at[idx, "Wikidata_MatchedLabel"] = matched
            df.at[idx, "Wikidata_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""
            df.at[idx, "Wikidata_MatchedYear"] = str(obj.get("Wikidata_ReleaseYear") or "").strip()


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

    def _run_tasks(task_fns: dict[str, callable[[], pd.DataFrame]]) -> dict[str, pd.DataFrame]:
        if not task_fns:
            return {}
        max_workers = min(
            len(task_fns),
            int(getattr(CLI, "max_parallel_providers", 8) or 8),
        )
        max_workers = max(1, max_workers)
        if max_workers == 1 or len(task_fns) == 1:
            return {k: fn() for k, fn in task_fns.items()}
        out: dict[str, pd.DataFrame] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(fn): name for name, fn in task_fns.items()}
            for fut in as_completed(futs):
                name = futs[fut]
                try:
                    out[name] = fut.result()
                except Exception as e:
                    raise SystemExit(f"Import provider task failed ({name}): {e}") from e
        return out

    def _merge_prefix_cols(
        dst: pd.DataFrame, src: pd.DataFrame, prefix: str, extras: tuple[str, ...]
    ) -> None:
        cols = [c for c in src.columns if c.startswith(prefix)]
        for c in extras:
            if c in src.columns and c not in cols:
                cols.append(c)
        for c in cols:
            if c not in dst.columns:
                continue
            dst[c] = src[c]

    # Stage 1 (fast): RAWG + IGDB in parallel.
    stage1: dict[str, callable[[], pd.DataFrame]] = {}
    rawg = clients.get("rawg")
    if "rawg" in ctx.sources and rawg is not None:
        stage1["rawg"] = lambda: (
            (
                lambda d: (
                    _match_rawg_ids(
                        d,
                        client=rawg,
                        include_diagnostics=include_diagnostics,
                        active_total=active_total,
                    ),
                    d,
                )[1]
            )(df.copy())  # type: ignore[arg-type]
        )
    igdb = clients.get("igdb")
    if "igdb" in ctx.sources and igdb is not None:
        stage1["igdb"] = lambda: (
            (
                lambda d: (
                    _match_igdb_ids(
                        d,
                        client=igdb,
                        include_diagnostics=include_diagnostics,
                        active_total=active_total,
                    ),
                    d,
                )[1]
            )(df.copy())  # type: ignore[arg-type]
        )

    for name, src_df in _run_tasks(stage1).items():
        if name == "rawg":
            _merge_prefix_cols(df, src_df, "RAWG_", ("RAWG_ID",))
        if name == "igdb":
            _merge_prefix_cols(df, src_df, "IGDB_", ("IGDB_ID",))

    # Stage 2: Steam + HLTB + Wikidata in parallel (Steam may infer from IGDB_ID).
    stage2: dict[str, callable[[], pd.DataFrame]] = {}
    steam = clients.get("steam")
    if "steam" in ctx.sources and steam is not None:
        stage2["steam"] = lambda: (
            (
                lambda d: (
                    _match_steam_appids(
                        d,
                        steam=steam,
                        igdb=igdb,
                        include_diagnostics=include_diagnostics,
                        active_total=active_total,
                    ),
                    d,
                )[1]
            )(df.copy())  # type: ignore[arg-type]
        )
    hltb = clients.get("hltb")
    if "hltb" in ctx.sources and hltb is not None:
        stage2["hltb"] = lambda: (
            (
                lambda d: (
                    _match_hltb_ids(
                        d,
                        client=hltb,
                        include_diagnostics=include_diagnostics,
                        active_total=active_total,
                    ),
                    d,
                )[1]
            )(df.copy())  # type: ignore[arg-type]
        )
    wikidata = clients.get("wikidata")
    if "wikidata" in ctx.sources and wikidata is not None:
        stage2["wikidata"] = lambda: (
            (
                lambda d: (
                    _match_wikidata_qids(
                        d,
                        client=wikidata,
                        include_diagnostics=include_diagnostics,
                        active_total=active_total,
                    ),
                    d,
                )[1]
            )(df.copy())  # type: ignore[arg-type]
        )

    for name, src_df in _run_tasks(stage2).items():
        if name == "steam":
            _merge_prefix_cols(df, src_df, "Steam_", ("Steam_AppID",))
        if name == "hltb":
            _merge_prefix_cols(df, src_df, "HLTB_", ("HLTB_ID", "HLTB_Query"))
        if name == "wikidata":
            _merge_prefix_cols(df, src_df, "Wikidata_", ("Wikidata_QID",))

    if include_diagnostics:
        df = fill_eval_tags(df, sources=set(ctx.sources), clients=clients)
    else:
        for c in ("ReviewTags", "MatchConfidence"):
            if c in df.columns:
                df[c] = ""

    log_cache_stats(clients)

    write_full_csv(df, output_csv)
    logging.info(f"✔ Import matching completed: {output_csv}")
    return output_csv
