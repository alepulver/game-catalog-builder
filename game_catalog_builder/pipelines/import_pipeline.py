from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, cast

import pandas as pd

from ..config import CLI
from ..metrics.registry import MetricsRegistry, default_metrics_registry_path, load_metrics_registry
from ..pipelines.artifacts import ArtifactStore
from ..pipelines.common import iter_named_rows_with_progress, log_cache_stats, write_full_csv
from ..pipelines.context import PipelineContext
from ..pipelines.protocols import (
    HLTBClientLike,
    IGDBClientLike,
    RAWGClientLike,
    SteamClientLike,
    WikidataClientLike,
)
from ..utils import (
    IDENTITY_NOT_FOUND,
    ensure_columns,
    ensure_row_ids,
    extract_year_hint,
    fuzzy_score,
    read_csv,
)
from .diagnostics.import_diagnostics import fill_eval_tags, platform_is_pc_like


def _default_metrics_registry_path() -> Path:
    return default_metrics_registry_path()


def _set_diag(
    df: pd.DataFrame,
    idx: int,
    *,
    registry: MetricsRegistry,
    key: str,
    value: object,
) -> None:
    mapped = registry.diagnostic_column_for_key(key)
    if mapped is None:
        return
    col, _typ = mapped
    if col not in df.columns:
        df[col] = ""
    df.at[idx, col] = value


def normalize_catalog(
    input_csv: Path,
    output_csv: Path,
    *,
    include_diagnostics: bool = True,
    registry: MetricsRegistry | None = None,
) -> Path:
    df = read_csv(input_csv)
    if "Name" not in df.columns:
        raise SystemExit(f"Missing required column 'Name' in {input_csv}")

    # Preserve RowId (and pinned provider identifiers) when re-importing from a user export that
    # doesn't include RowId yet.
    if output_csv.exists() and input_csv.resolve() != output_csv.resolve() and "RowId" not in df.columns:
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
        reg = registry or load_metrics_registry(_default_metrics_registry_path())
        required_cols.update({c: "" for c in sorted(reg.diagnostic_columns)})

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
    registry: MetricsRegistry | None,
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
            if include_diagnostics and registry is not None:
                _set_diag(df, int(idx), registry=registry, key="diagnostics.rawg.matched_name", value="")
                _set_diag(df, int(idx), registry=registry, key="diagnostics.rawg.match_score", value="")
                _set_diag(df, int(idx), registry=registry, key="diagnostics.rawg.matched_year", value="")
            continue

        if rawg_id:
            if not include_diagnostics:
                continue
            obj = client.get_by_id(rawg_id)
        else:
            obj = client.search(name, year_hint=_year_hint_from_row(row))
            if obj and obj.get("id") is not None:
                df.at[idx, "RAWG_ID"] = str(obj.get("id") or "").strip()

        if include_diagnostics and registry is not None and obj and isinstance(obj, dict):
            matched = str(obj.get("name") or "").strip()
            released = str(obj.get("released") or "").strip()
            _set_diag(df, int(idx), registry=registry, key="diagnostics.rawg.matched_name", value=matched)
            _set_diag(
                df,
                int(idx),
                registry=registry,
                key="diagnostics.rawg.match_score",
                value=int(fuzzy_score(name, matched)) if matched else "",
            )
            _set_diag(
                df,
                int(idx),
                registry=registry,
                key="diagnostics.rawg.matched_year",
                value=int(released[:4]) if released[:4].isdigit() else "",
            )


def _match_igdb_ids(
    df: pd.DataFrame,
    *,
    client: IGDBClientLike,
    include_diagnostics: bool,
    registry: MetricsRegistry | None,
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
            if include_diagnostics and registry is not None:
                _set_diag(df, int(idx), registry=registry, key="diagnostics.igdb.matched_name", value="")
                _set_diag(df, int(idx), registry=registry, key="diagnostics.igdb.match_score", value="")
                _set_diag(df, int(idx), registry=registry, key="diagnostics.igdb.matched_year", value="")
            continue

        if igdb_id:
            if not include_diagnostics:
                continue
            obj = client.get_by_id(igdb_id)
        else:
            obj = client.search(name, year_hint=_year_hint_from_row(row))
            if obj and str(obj.get("igdb.id", "") or "").strip():
                df.at[idx, "IGDB_ID"] = str(obj.get("igdb.id") or "").strip()

        if include_diagnostics and registry is not None and obj and isinstance(obj, dict):
            matched = str(obj.get("igdb.name") or "").strip()
            _set_diag(df, int(idx), registry=registry, key="diagnostics.igdb.matched_name", value=matched)
            _set_diag(
                df,
                int(idx),
                registry=registry,
                key="diagnostics.igdb.match_score",
                value=int(fuzzy_score(name, matched)) if matched else "",
            )
            year = obj.get("igdb.year")
            _set_diag(
                df,
                int(idx),
                registry=registry,
                key="diagnostics.igdb.matched_year",
                value=int(year) if isinstance(year, int) else "",
            )


def _match_steam_appids(
    df: pd.DataFrame,
    *,
    steam: SteamClientLike,
    igdb: IGDBClientLike | None,
    include_diagnostics: bool,
    registry: MetricsRegistry | None,
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
        if include_diagnostics and registry is not None:
            _set_diag(
                df,
                int(row_idx),
                registry=registry,
                key="diagnostics.steam.matched_name",
                value=matched,
            )
            _set_diag(
                df,
                int(row_idx),
                registry=registry,
                key="diagnostics.steam.match_score",
                value=int(fuzzy_score(personal, matched)) if matched else "",
            )
            _set_diag(
                df,
                int(row_idx),
                registry=registry,
                key="diagnostics.steam.matched_year",
                value=extract_year_hint(date) or "",
            )

    for idx, row, name, _seen in iter_named_rows_with_progress(
        df,
        label="STEAM",
        total=active_total,
        skip_row=lambda r: _is_yes(r.get("Disabled", "")),
    ):
        steam_id = str(row.get("Steam_AppID", "") or "").strip()
        if not platform_is_pc_like(row.get("Platform", "")) and not steam_id:
            if include_diagnostics and registry is not None:
                _set_diag(df, int(idx), registry=registry, key="diagnostics.steam.matched_name", value="")
                _set_diag(df, int(idx), registry=registry, key="diagnostics.steam.match_score", value="")
                _set_diag(df, int(idx), registry=registry, key="diagnostics.steam.matched_year", value="")
            continue

        if steam_id == IDENTITY_NOT_FOUND:
            if include_diagnostics and registry is not None:
                _set_diag(df, int(idx), registry=registry, key="diagnostics.steam.matched_name", value="")
                _set_diag(df, int(idx), registry=registry, key="diagnostics.steam.match_score", value="")
                _set_diag(df, int(idx), registry=registry, key="diagnostics.steam.matched_year", value="")
                _set_diag(
                    df,
                    int(idx),
                    registry=registry,
                    key="diagnostics.steam.rejected_reason",
                    value="",
                )
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
                inferred = str((igdb_obj or {}).get("igdb.cross_ids.steam_app_id") or "").strip()
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
        if include_diagnostics and registry is not None:
            _set_diag(df, int(idx), registry=registry, key="diagnostics.steam.matched_name", value=matched)
            _set_diag(
                df,
                int(idx),
                registry=registry,
                key="diagnostics.steam.match_score",
                value=int(fuzzy_score(name, matched)) if matched else "",
            )
            _set_diag(
                df,
                int(idx),
                registry=registry,
                key="diagnostics.steam.matched_year",
                value=(res or {}).get("release_year") or "",
            )
            _set_diag(
                df,
                int(idx),
                registry=registry,
                key="diagnostics.steam.rejected_reason",
                value=str((res or {}).get("rejected_reason") or "").strip(),
            )


def _match_hltb_ids(
    df: pd.DataFrame,
    *,
    client: HLTBClientLike,
    include_diagnostics: bool,
    registry: MetricsRegistry | None,
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
            if include_diagnostics and registry is not None:
                _set_diag(df, int(idx), registry=registry, key="diagnostics.hltb.matched_name", value="")
                _set_diag(df, int(idx), registry=registry, key="diagnostics.hltb.match_score", value="")
                _set_diag(df, int(idx), registry=registry, key="diagnostics.hltb.matched_year", value="")
                _set_diag(
                    df,
                    int(idx),
                    registry=registry,
                    key="diagnostics.hltb.matched_platforms",
                    value="",
                )
            continue

        if hltb_id:
            if not include_diagnostics:
                continue
            obj = client.get_by_id(hltb_id)
            if include_diagnostics and registry is not None and obj and isinstance(obj, dict):
                matched = str(obj.get("hltb.name") or "").strip()
                _set_diag(
                    df,
                    int(idx),
                    registry=registry,
                    key="diagnostics.hltb.matched_name",
                    value=matched,
                )
                _set_diag(
                    df,
                    int(idx),
                    registry=registry,
                    key="diagnostics.hltb.match_score",
                    value=int(fuzzy_score(name, matched)) if matched else "",
                )
                year = obj.get("hltb.release_year")
                _set_diag(
                    df,
                    int(idx),
                    registry=registry,
                    key="diagnostics.hltb.matched_year",
                    value=int(year) if isinstance(year, int) else "",
                )
                platforms = obj.get("hltb.platforms", [])
                if isinstance(platforms, list):
                    _set_diag(
                        df,
                        int(idx),
                        registry=registry,
                        key="diagnostics.hltb.matched_platforms",
                        value=[str(p or "").strip() for p in platforms if str(p or "").strip()],
                    )
            continue

        res = client.search(name, query=hltb_query, hltb_id=None)
        if res and isinstance(res, dict):
            rid = str(res.get("hltb.id") or "").strip()
            if rid and rid.casefold() != "nan":
                df.at[idx, "HLTB_ID"] = rid
            df.at[idx, "HLTB_Query"] = hltb_query
            if include_diagnostics and registry is not None:
                matched = str(res.get("hltb.name") or "").strip()
                _set_diag(
                    df,
                    int(idx),
                    registry=registry,
                    key="diagnostics.hltb.matched_name",
                    value=matched,
                )
                _set_diag(
                    df,
                    int(idx),
                    registry=registry,
                    key="diagnostics.hltb.match_score",
                    value=int(fuzzy_score(name, matched)) if matched else "",
                )
                year = res.get("hltb.release_year")
                _set_diag(
                    df,
                    int(idx),
                    registry=registry,
                    key="diagnostics.hltb.matched_year",
                    value=int(year) if isinstance(year, int) else "",
                )
                platforms = res.get("hltb.platforms", [])
                if isinstance(platforms, list):
                    _set_diag(
                        df,
                        int(idx),
                        registry=registry,
                        key="diagnostics.hltb.matched_platforms",
                        value=[str(p or "").strip() for p in platforms if str(p or "").strip()],
                    )


def _match_wikidata_qids(
    df: pd.DataFrame,
    *,
    client: WikidataClientLike,
    include_diagnostics: bool,
    registry: MetricsRegistry | None,
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
            if include_diagnostics and registry is not None:
                _set_diag(
                    df,
                    int(idx),
                    registry=registry,
                    key="diagnostics.wikidata.matched_label",
                    value="",
                )
                _set_diag(
                    df,
                    int(idx),
                    registry=registry,
                    key="diagnostics.wikidata.match_score",
                    value="",
                )
                _set_diag(
                    df,
                    int(idx),
                    registry=registry,
                    key="diagnostics.wikidata.matched_year",
                    value="",
                )
            continue
        if qid:
            if not include_diagnostics:
                continue
            obj = client.get_by_id(qid)
        else:
            obj = client.search(name, year_hint=_year_hint_from_row(row))
            if obj and str(obj.get("wikidata.qid", "") or "").strip():
                df.at[idx, "Wikidata_QID"] = str(obj.get("wikidata.qid") or "").strip()
        if include_diagnostics and registry is not None and obj and isinstance(obj, dict):
            matched = str(obj.get("wikidata.label") or "").strip()
            _set_diag(
                df,
                int(idx),
                registry=registry,
                key="diagnostics.wikidata.matched_label",
                value=matched,
            )
            _set_diag(
                df,
                int(idx),
                registry=registry,
                key="diagnostics.wikidata.match_score",
                value=int(fuzzy_score(name, matched)) if matched else "",
            )
            year = obj.get("wikidata.release_year")
            _set_diag(
                df,
                int(idx),
                registry=registry,
                key="diagnostics.wikidata.matched_year",
                value=int(year) if isinstance(year, int) else "",
            )


def run_import(
    ctx: PipelineContext,
    *,
    input_csv: Path,
    output_csv: Path,
    include_diagnostics: bool,
    write_jsonl: bool = True,
) -> Path:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    ctx.cache_dir.mkdir(parents=True, exist_ok=True)

    registry: MetricsRegistry | None = None
    if include_diagnostics or write_jsonl:
        registry = load_metrics_registry(_default_metrics_registry_path())

    normalize_catalog(input_csv, output_csv, include_diagnostics=include_diagnostics, registry=registry)
    df = read_csv(output_csv)
    _ensure_year_hint_column(df)

    clients = ctx.build_clients()
    active_total = sum(1 for _, r in df.iterrows() if not _is_yes(r.get("Disabled", "")))

    def _run_tasks(task_fns: dict[str, Callable[[], pd.DataFrame]]) -> dict[str, pd.DataFrame]:
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
    stage1: dict[str, Callable[[], pd.DataFrame]] = {}
    rawg = clients.get("rawg")
    if "rawg" in ctx.sources and rawg is not None:
        rawg_client = cast(RAWGClientLike, rawg)
        stage1["rawg"] = lambda: (
            (
                lambda d: (
                    _match_rawg_ids(
                        d,
                        client=rawg_client,
                        include_diagnostics=include_diagnostics,
                        registry=registry,
                        active_total=active_total,
                    ),
                    d,
                )[1]
            )(df.copy())  # type: ignore[arg-type]
        )
    igdb = clients.get("igdb")
    if "igdb" in ctx.sources and igdb is not None:
        igdb_client = cast(IGDBClientLike, igdb)
        stage1["igdb"] = lambda: (
            (
                lambda d: (
                    _match_igdb_ids(
                        d,
                        client=igdb_client,
                        include_diagnostics=include_diagnostics,
                        registry=registry,
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
    stage2: dict[str, Callable[[], pd.DataFrame]] = {}
    steam = clients.get("steam")
    if "steam" in ctx.sources and steam is not None:
        steam_client = cast(SteamClientLike, steam)
        stage2["steam"] = lambda: (
            (
                lambda d: (
                    _match_steam_appids(
                        d,
                        steam=steam_client,
                        igdb=cast(IGDBClientLike, igdb) if igdb is not None else None,
                        include_diagnostics=include_diagnostics,
                        registry=registry,
                        active_total=active_total,
                    ),
                    d,
                )[1]
            )(df.copy())  # type: ignore[arg-type]
        )
    hltb = clients.get("hltb")
    if "hltb" in ctx.sources and hltb is not None:
        hltb_client = cast(HLTBClientLike, hltb)
        stage2["hltb"] = lambda: (
            (
                lambda d: (
                    _match_hltb_ids(
                        d,
                        client=hltb_client,
                        include_diagnostics=include_diagnostics,
                        registry=registry,
                        active_total=active_total,
                    ),
                    d,
                )[1]
            )(df.copy())  # type: ignore[arg-type]
        )
    wikidata = clients.get("wikidata")
    if "wikidata" in ctx.sources and wikidata is not None:
        wikidata_client = cast(WikidataClientLike, wikidata)
        stage2["wikidata"] = lambda: (
            (
                lambda d: (
                    _match_wikidata_qids(
                        d,
                        client=wikidata_client,
                        include_diagnostics=include_diagnostics,
                        registry=registry,
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
        df = fill_eval_tags(df, sources=set(ctx.sources), clients=clients, registry=registry)
    else:
        for c in ("ReviewTags", "MatchConfidence"):
            if c in df.columns:
                df[c] = ""

    log_cache_stats(clients)

    artifacts = ArtifactStore(
        run_dir=output_csv.parent,
        registry=registry,
        use_jsonl=write_jsonl,
        reuse_jsonl=False,
        jsonl_dir=output_csv.parent / "jsonl",
    )
    artifacts.write_catalog(df, output_csv)
    logging.info(f"✔ Import matching completed: {output_csv}")

    return output_csv
