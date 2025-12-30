from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from typing import TYPE_CHECKING, Mapping, cast

import pandas as pd

from ..clients import (
    HLTBClient,
    IGDBClient,
    RAWGClient,
    SteamClient,
    SteamSpyClient,
    WikidataClient,
    WikipediaPageviewsClient,
    WikipediaSummaryClient,
)
from ..config import CLI, IGDB, RAWG, STEAM, STEAMSPY, WIKIDATA
from ..metrics.registry import MetricsRegistry, default_metrics_registry_path, load_metrics_registry
from ..schema import PROVIDER_PREFIXES, PUBLIC_DEFAULT_COLS
from ..utils import (
    IDENTITY_NOT_FOUND,
    ensure_columns,
    generate_validation_report,
    is_row_processed,
    load_credentials,
    load_identity_overrides,
    load_json_cache,
    normalize_game_name,
    read_csv,
)
from ..utils.merger import reorder_columns
from ..utils.progress import Progress
from ..utils.signals import apply_phase1_signals
from .common import (
    flush_pending_keys,
    iter_named_rows_with_progress,
    log_cache_stats,
    total_named_rows,
    write_full_csv,
    write_provider_output_csv,
)
from .artifacts import ArtifactStore

if TYPE_CHECKING:
    from .context import PipelineContext


def apply_registered_metrics(
    df: pd.DataFrame,
    *,
    idx: int,
    metrics: Mapping[str, object],
    registry: MetricsRegistry,
    label: str,
) -> None:
    for key, v in metrics.items():
        mapped = registry.column_for_key(key)
        if mapped is None:
            continue
        col, _typ = mapped
        if col not in df.columns:
            df[col] = ""
        df.at[idx, col] = v


def drop_eval_columns(df: pd.DataFrame, *, diagnostic_columns: set[str]) -> pd.DataFrame:
    cols = [c for c in sorted(diagnostic_columns) if c in df.columns]
    return df.drop(columns=cols) if cols else df


def clear_prefixed_columns(df: pd.DataFrame, idx: int, prefix: str) -> None:
    for c in [col for col in df.columns if col.startswith(prefix)]:
        df.at[idx, c] = ""


def load_or_merge_dataframe(
    input_csv: Path,
    output_csv: Path,
    *,
    keep_cols_from_output: set[str] | None = None,
) -> pd.DataFrame:
    """
    Load dataframe from input CSV, optionally merging a small set of columns from an existing
    output CSV.

    Output CSVs are a presentation layer, so we only merge scalar "resume" columns (e.g. pinned
    IDs) and never attempt to parse multi-value cells back into structured values.
    """
    df = read_csv(input_csv)
    if "RowId" not in df.columns:
        raise SystemExit(f"Missing RowId in {input_csv}; run `import` first.")

    if output_csv.exists() and keep_cols_from_output:
        df_output = read_csv(output_csv)
        if "RowId" not in df_output.columns:
            raise SystemExit(f"Missing RowId in {output_csv}; delete it and re-run, or regenerate outputs.")
        keep = {"RowId"} | {c for c in keep_cols_from_output if c in df_output.columns}
        df_output = df_output[list(keep)]
        df = df.merge(df_output, on="RowId", how="left", suffixes=("", "_existing"))
        for col in list(df.columns):
            if not col.endswith("_existing"):
                continue
            original_col = col[: -len("_existing")]
            if original_col in df.columns:
                mask = (df[col].notna()) & (df[col] != "")
                df.loc[mask, original_col] = df.loc[mask, col]
            df = df.drop(columns=[col])

    df = ensure_columns(df, PUBLIC_DEFAULT_COLS)
    return df


def build_personal_base_for_enrich(df: pd.DataFrame, *, diagnostic_columns: set[str]) -> pd.DataFrame:
    """
    Prepare the base dataframe for a fresh merge by removing provider-derived columns.

    This is critical for "in-place" enrich (input == output) to avoid keeping stale provider
    columns: the merge overlay does not overwrite existing same-named columns.
    """
    from ..schema import PINNED_ID_COLS

    drop_prefixes = ("RAWG_", "IGDB_", "Steam_", "SteamSpy_", "HLTB_", "Wikidata_")
    keep = set(PINNED_ID_COLS) | {"RowId", "Name"}
    drop_eval = set(diagnostic_columns)

    cols: list[str] = []
    for c in df.columns:
        if c in drop_eval:
            continue
        if c in keep:
            cols.append(c)
            continue
        if c.startswith(drop_prefixes):
            continue
        cols.append(c)
    out_df = cast(pd.DataFrame, df[cols]).copy()
    return out_df


def process_steam_and_steamspy_streaming(
    *,
    input_csv: Path,
    steam_output_csv: Path,
    steamspy_output_csv: Path,
    steam_cache_path: Path,
    steamspy_cache_path: Path,
    registry: MetricsRegistry,
    identity_overrides: dict[str, dict[str, str]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    def _clean_str(value: object) -> str:
        # Pandas may store empty cells as NaN floats; avoid propagating "nan".
        if isinstance(value, (pd.Series, pd.DataFrame)):
            return ""
        try:
            if bool(pd.isna(value)):
                return ""
        except Exception:
            pass
        s = str(value or "").strip()
        return "" if s.casefold() == "nan" else s

    steam_client = SteamClient(cache_path=steam_cache_path, min_interval_s=STEAM.storesearch_min_interval_s)
    steamspy_client = SteamSpyClient(cache_path=steamspy_cache_path, min_interval_s=STEAMSPY.min_interval_s)

    df_steam = load_or_merge_dataframe(
        input_csv,
        steam_output_csv,
        keep_cols_from_output={"Steam_AppID", "Steam_Name"},
    )
    df_steamspy = load_or_merge_dataframe(
        input_csv,
        steamspy_output_csv,
        keep_cols_from_output={"SteamSpy_Owners"},
    )
    total_steam_rows = total_named_rows(df_steam)
    q: Queue[tuple[int, str, str] | None] = Queue()
    steamspy_progress = {"enqueued": 0, "done": 0}

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
                for idx2 in indices:
                    apply_registered_metrics(
                        df_steam,
                        idx=int(idx2),
                        metrics=steam_client.extract_metrics(int(appid), details),
                        registry=registry,
                        label="STEAM",
                    )
                processed += len(indices)
            pending.clear()

        for idx, row, name, _ in iter_named_rows_with_progress(
            df_steam, label="STEAM", total=total_steam_rows
        ):
            rowid = _clean_str(row.get("RowId", ""))
            override_appid = ""
            if identity_overrides and rowid:
                override_appid = _clean_str(identity_overrides.get(rowid, {}).get("Steam_AppID", ""))

            if override_appid == IDENTITY_NOT_FOUND:
                clear_prefixed_columns(df_steam, int(idx), "Steam_")
                continue

            if is_row_processed(df_steam, int(idx), ["Steam_Name"]):
                current_appid = _clean_str(df_steam.at[idx, "Steam_AppID"])
                if current_appid and not is_row_processed(df_steamspy, int(idx), ["SteamSpy_Owners"]):
                    q.put((int(idx), name, current_appid))
                    steamspy_progress["enqueued"] += 1
                if not (override_appid and current_appid != override_appid):
                    continue

            if override_appid:
                appid = override_appid
            else:
                logging.debug(f"[STEAM] Processing: {name}")
                search = steam_client.search_appid(name)
                if not search:
                    continue
                appid = _clean_str(search.get("id"))
                if not appid:
                    continue

            df_steam.at[idx, "Steam_AppID"] = appid
            q.put((int(idx), name, appid))
            steamspy_progress["enqueued"] += 1

            try:
                appid_int = int(appid)
            except ValueError:
                continue

            cached_details = steam_client.get_app_details(appid_int)
            if cached_details:
                apply_registered_metrics(
                    df_steam,
                    idx=int(idx),
                    metrics=steam_client.extract_metrics(appid_int, cached_details),
                    registry=registry,
                    label="STEAM",
                )
                processed += 1
            else:
                pending.setdefault(appid_int, []).append(int(idx))

            if processed % 10 == 0:
                write_provider_output_csv(df_steam, steam_output_csv, prefix="Steam_")

            if len(pending) >= CLI.steam_streaming_flush_batch_size:
                _flush_pending()
                write_provider_output_csv(df_steam, steam_output_csv, prefix="Steam_")

        _flush_pending()
        write_provider_output_csv(df_steam, steam_output_csv, prefix="Steam_")
        q.put(None)

    def steamspy_consumer() -> None:
        processed = 0
        progress = Progress("STEAMSPY", total=None, every_n=CLI.progress_every_n)
        last_log = time.time()
        while True:
            item = q.get()
            if item is None:
                break
            idx, name, appid = item
            if is_row_processed(df_steamspy, idx, ["SteamSpy_Owners"]):
                continue
            logging.debug(f"[STEAMSPY] {name} (AppID {appid})")
            try:
                appid_int = int(str(appid).strip())
            except Exception:
                logging.warning(f"[STEAMSPY] Skipping invalid AppID for '{name}': {appid!r}")
                continue
            data = steamspy_client.fetch(appid_int)
            if not data:
                continue
            apply_registered_metrics(
                df_steamspy,
                idx=int(idx),
                metrics=data,
                registry=registry,
                label="STEAMSPY",
            )
            processed += 1
            steamspy_progress["done"] += 1
            progress.maybe_log(processed)
            now = time.time()
            if now - last_log >= float(getattr(CLI, "progress_min_interval_s", 30.0) or 30.0):
                last_log = now
                try:
                    qsize = q.qsize()
                except Exception:
                    qsize = -1
                enq = int(steamspy_progress.get("enqueued", 0) or 0)
                done = int(steamspy_progress.get("done", 0) or 0)
                total = f"{done}/{enq}" if enq else f"{done}"
                qmsg = f" queue={qsize}" if qsize >= 0 else ""
                logging.info(f"[STEAMSPY] Queue progress {total} tasks{qmsg}")
            if processed % 10 == 0:
                write_provider_output_csv(
                    df_steamspy,
                    steamspy_output_csv,
                    prefix="SteamSpy_",
                    extra=("SteamSpy_Score_100",),
                )

        write_provider_output_csv(
            df_steamspy,
            steamspy_output_csv,
            prefix="SteamSpy_",
            extra=("SteamSpy_Score_100",),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(steam_producer)
        f2 = executor.submit(steamspy_consumer)
        f1.result()
        f2.result()

    log_cache_stats({"steam": steam_client, "steamspy": steamspy_client})
    return df_steam, df_steamspy


def process_igdb(
    *,
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    credentials: dict,
    required_cols: list[str],
    registry: MetricsRegistry,
    language: str = "en",
    identity_overrides: dict[str, dict[str, str]] | None = None,
) -> pd.DataFrame:
    client = IGDBClient(
        client_id=credentials.get("igdb", {}).get("client_id", ""),
        client_secret=credentials.get("igdb", {}).get("client_secret", ""),
        cache_path=cache_path,
        language=language,
        min_interval_s=IGDB.min_interval_s,
    )
    df = load_or_merge_dataframe(input_csv, output_csv)
    total_rows = total_named_rows(df)

    processed = 0
    pending_by_id: dict[object, list[int]] = {}

    def _apply_igdb_fields(_igdb_id: object, indices: list[int], data: object) -> int:
        if not isinstance(data, dict):
            return 0
        for idx2 in indices:
            apply_registered_metrics(df, idx=int(idx2), metrics=data, registry=registry, label="IGDB")
        return len(indices)

    def _flush_pending() -> None:
        nonlocal processed
        processed += flush_pending_keys(
            pending_by_id,
            fetch_many=lambda keys: client.get_by_ids([str(k) for k in keys if str(k).strip()]),
            on_item=_apply_igdb_fields,
        )

    for idx, row, name, _seen in iter_named_rows_with_progress(df, label="IGDB", total=total_rows):
        rowid = str(row.get("RowId", "") or "").strip()
        override_id = ""
        if identity_overrides and rowid:
            override_id = str(identity_overrides.get(rowid, {}).get("IGDB_ID", "") or "").strip()

        if override_id == IDENTITY_NOT_FOUND:
            clear_prefixed_columns(df, int(idx), "IGDB_")
            continue

        if is_row_processed(df, idx, required_cols):
            if not (override_id and str(df.at[idx, "IGDB_ID"] or "").strip() != override_id):
                continue

        if override_id:
            df.at[idx, "IGDB_ID"] = str(override_id).strip()
            pending_by_id.setdefault(str(override_id).strip(), []).append(int(idx))
        else:
            igdb_id = str(df.at[idx, "IGDB_ID"] or "").strip()
            if igdb_id:
                pending_by_id.setdefault(igdb_id, []).append(int(idx))
            else:
                logging.debug(f"[IGDB] Processing: {name}")
                data = client.search(name)
                if not data:
                    continue
                    apply_registered_metrics(df, idx=idx, metrics=data, registry=registry, label="IGDB")
                    processed += 1

        if processed % 10 == 0:
            write_provider_output_csv(
                df,
                output_csv,
                prefix="IGDB_",
                extra=("IGDB_Score_100", "IGDB_CriticScore_100"),
            )

        if len(pending_by_id) >= CLI.igdb_flush_batch_size:
            _flush_pending()
            write_provider_output_csv(
                df,
                output_csv,
                prefix="IGDB_",
                extra=("IGDB_Score_100", "IGDB_CriticScore_100"),
            )

    _flush_pending()
    write_provider_output_csv(
        df,
        output_csv,
        prefix="IGDB_",
        extra=("IGDB_Score_100", "IGDB_CriticScore_100"),
    )
    log_cache_stats({"igdb": client})
    logging.info(f"✔ IGDB completed: {output_csv}")
    return df


def process_rawg(
    *,
    input_csv: Path,
    output_csv: Path | None,
    cache_path: Path,
    credentials: dict,
    required_cols: list[str],
    registry: MetricsRegistry,
    language: str = "en",
    identity_overrides: dict[str, dict[str, str]] | None = None,
    row_filter: set[str] | None = None,
) -> pd.DataFrame:
    client = RAWGClient(
        api_key=credentials.get("rawg", {}).get("api_key", ""),
        cache_path=cache_path,
        language=language,
        min_interval_s=RAWG.min_interval_s,
    )
    df = load_or_merge_dataframe(input_csv, output_csv) if output_csv else read_csv(input_csv)
    if row_filter:
        from .common import filter_rows_by_ids

        df = filter_rows_by_ids(df, row_filter)
    total_rows = total_named_rows(df)

    processed = 0
    for idx, row, name, _seen in iter_named_rows_with_progress(df, label="RAWG", total=total_rows):
        rowid = str(row.get("RowId", "") or "").strip()
        override_id = ""
        if identity_overrides and rowid:
            override_id = str(identity_overrides.get(rowid, {}).get("RAWG_ID", "") or "").strip()

        if override_id == IDENTITY_NOT_FOUND:
            clear_prefixed_columns(df, int(idx), "RAWG_")
            continue

        if is_row_processed(df, idx, required_cols):
            if not (override_id and str(df.at[idx, "RAWG_ID"] or "").strip() != override_id):
                continue

        if override_id:
            result = client.get_by_id(override_id)
            if not result:
                logging.warning(f"[RAWG] Override id not found: {name} (RAWG_ID {override_id})")
                continue
        else:
            logging.debug(f"[RAWG] Processing: {name}")
            result = client.search(name)
            if not result:
                continue

        apply_registered_metrics(
            df,
            idx=idx,
            metrics=client.extract_metrics(result),
            registry=registry,
            label="RAWG",
        )

        processed += 1
        if output_csv and processed % 10 == 0:
            write_provider_output_csv(df, output_csv, prefix="RAWG_", extra=("RAWG_Score_100",))

    if output_csv:
        write_provider_output_csv(df, output_csv, prefix="RAWG_", extra=("RAWG_Score_100",))
    log_cache_stats({"rawg": client})
    logging.info(f"✔ RAWG completed: {output_csv}")
    return df


def process_steam(
    *,
    input_csv: Path,
    output_csv: Path | None,
    cache_path: Path,
    required_cols: list[str],
    registry: MetricsRegistry,
    identity_overrides: dict[str, dict[str, str]] | None = None,
    row_filter: set[str] | None = None,
) -> pd.DataFrame:
    client = SteamClient(cache_path=cache_path, min_interval_s=STEAM.storesearch_min_interval_s)
    df = load_or_merge_dataframe(input_csv, output_csv) if output_csv else read_csv(input_csv)
    if row_filter:
        from .common import filter_rows_by_ids

        df = filter_rows_by_ids(df, row_filter)
    total_rows = total_named_rows(df)

    pending: dict[object, list[int]] = {}

    def _apply_steam_fields(appid: object, indices: list[int], details: object) -> int:
        if not isinstance(details, dict):
            return 0
        appid_int = int(appid) if isinstance(appid, int) else int(str(appid).strip())
        for idx2 in indices:
            apply_registered_metrics(
                df,
                idx=int(idx2),
                metrics=client.extract_metrics(appid_int, details),
                registry=registry,
                label="STEAM",
            )
        return len(indices)

    def _flush_pending() -> None:
        flush_pending_keys(
            pending,
            fetch_many=lambda keys: client.get_app_details_many(
                [int(str(k).strip()) for k in keys if str(k).strip().isdigit()]
            ),
            on_item=_apply_steam_fields,
        )

    queued = 0
    for idx, row, name, _seen in iter_named_rows_with_progress(df, label="STEAM", total=total_rows):
        rowid = str(row.get("RowId", "") or "").strip()
        override_appid = ""
        if identity_overrides and rowid:
            override_appid = str(identity_overrides.get(rowid, {}).get("Steam_AppID", "") or "").strip()

        if override_appid == IDENTITY_NOT_FOUND:
            clear_prefixed_columns(df, int(idx), "Steam_")
            continue

        if is_row_processed(df, idx, required_cols):
            if not (override_appid and str(df.at[idx, "Steam_AppID"] or "").strip() != override_appid):
                continue

        appid_str = override_appid or str(row.get("Steam_AppID", "") or "").strip()
        if not appid_str:
            logging.debug(f"[STEAM] Processing: {name}")
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

        if output_csv and queued % 10 == 0:
            write_provider_output_csv(df, output_csv, prefix="Steam_")

        if len(pending) >= CLI.steam_flush_batch_size:
            _flush_pending()
            if output_csv:
                write_provider_output_csv(df, output_csv, prefix="Steam_")

    _flush_pending()
    if output_csv:
        write_provider_output_csv(df, output_csv, prefix="Steam_")
    log_cache_stats({"steam": client})
    logging.info(f"✔ Steam completed: {output_csv}")
    return df


def process_steamspy(
    *,
    input_csv: Path,
    output_csv: Path | None,
    cache_path: Path,
    required_cols: list[str],
    registry: MetricsRegistry,
    row_filter: set[str] | None = None,
) -> pd.DataFrame:
    client = SteamSpyClient(cache_path=cache_path, min_interval_s=STEAMSPY.min_interval_s)
    if not input_csv.exists():
        raise FileNotFoundError(f"{input_csv} not found. Run steam processing first.")
    df = load_or_merge_dataframe(input_csv, output_csv) if output_csv else read_csv(input_csv)
    if row_filter:
        from .common import filter_rows_by_ids

        df = filter_rows_by_ids(df, row_filter)
    appids = df["Steam_AppID"] if "Steam_AppID" in df.columns else pd.Series([], dtype=str)
    total_rows = int((appids.astype(str).str.strip() != "").sum())

    processed = 0
    for idx, row, name, _ in iter_named_rows_with_progress(
        df,
        label="STEAMSPY",
        total=total_rows,
        skip_row=lambda r: not str(r.get("Steam_AppID", "") or "").strip(),
    ):
        appid = str(row.get("Steam_AppID", "") or "").strip()
        if is_row_processed(df, idx, required_cols):
            continue
        logging.debug(f"[STEAMSPY] {name} (AppID {appid})")
        data = client.fetch(int(appid))
        if not data:
            logging.warning(f"  ↳ No data in SteamSpy: {name} (AppID {appid})")
            continue
        apply_registered_metrics(df, idx=idx, metrics=data, registry=registry, label="STEAMSPY")
        processed += 1
        if output_csv and processed % 10 == 0:
            write_provider_output_csv(df, output_csv, prefix="SteamSpy_", extra=("SteamSpy_Score_100",))

    if output_csv:
        write_provider_output_csv(df, output_csv, prefix="SteamSpy_", extra=("SteamSpy_Score_100",))
    log_cache_stats({"steamspy": client})
    logging.info(f"✔ SteamSpy completed: {output_csv}")
    return df


def process_hltb(
    *,
    input_csv: Path,
    output_csv: Path | None,
    cache_path: Path,
    required_cols: list[str],
    registry: MetricsRegistry,
    identity_overrides: dict[str, dict[str, str]] | None = None,
    row_filter: set[str] | None = None,
) -> pd.DataFrame:
    def _clean_str(value: object) -> str:
        if isinstance(value, (pd.Series, pd.DataFrame)):
            return ""
        try:
            if bool(pd.isna(value)):
                return ""
        except Exception:
            pass
        s = str(value or "").strip()
        return "" if s.casefold() == "nan" else s

    client = HLTBClient(cache_path=cache_path)
    df = load_or_merge_dataframe(input_csv, output_csv) if output_csv else read_csv(input_csv)
    if row_filter:
        from .common import filter_rows_by_ids

        df = filter_rows_by_ids(df, row_filter)
    total_rows = total_named_rows(df)

    processed = 0
    for idx, row, name, _seen in iter_named_rows_with_progress(df, label="HLTB", total=total_rows):
        rowid = _clean_str(row.get("RowId", ""))
        pinned_id = _clean_str(row.get("HLTB_ID", ""))
        query = _clean_str(row.get("HLTB_Query", ""))
        if identity_overrides and rowid:
            pinned_id = _clean_str(identity_overrides.get(rowid, {}).get("HLTB_ID", "")) or pinned_id
            query = _clean_str(identity_overrides.get(rowid, {}).get("HLTB_Query", "")) or query
        if pinned_id == IDENTITY_NOT_FOUND or query == IDENTITY_NOT_FOUND:
            clear_prefixed_columns(df, int(idx), "HLTB_")
            continue
        query = query or name

        if is_row_processed(df, idx, required_cols):
            prev_name = str(df.at[idx, "HLTB_Name"] or "").strip()
            if prev_name and normalize_game_name(prev_name) == normalize_game_name(query):
                continue

        logging.debug(f"[HLTB] Processing: {query}")
        data = client.search(name, query=query, hltb_id=pinned_id or None)
        if not data:
            continue
        apply_registered_metrics(df, idx=idx, metrics=data, registry=registry, label="HLTB")

        processed += 1
        if output_csv and processed % 10 == 0:
            write_provider_output_csv(df, output_csv, prefix="HLTB_", extra=("HLTB_Score_100",))

    if output_csv:
        write_provider_output_csv(df, output_csv, prefix="HLTB_", extra=("HLTB_Score_100",))
    log_cache_stats({"hltb": client})
    logging.info(f"✔ HLTB completed: {output_csv}")
    return df


def process_wikidata(
    *,
    input_csv: Path,
    output_csv: Path | None,
    cache_path: Path,
    required_cols: list[str],
    registry: MetricsRegistry,
    identity_overrides: dict[str, dict[str, str]] | None = None,
    row_filter: set[str] | None = None,
) -> pd.DataFrame:
    client = WikidataClient(cache_path=cache_path, min_interval_s=WIKIDATA.min_interval_s)
    pageviews_client = WikipediaPageviewsClient(
        cache_path=cache_path.parent / "wiki_pageviews_cache.json", min_interval_s=0.15
    )
    summary_client = WikipediaSummaryClient(
        cache_path=cache_path.parent / "wiki_summary_cache.json", min_interval_s=0.15
    )
    cache_dir = cache_path.parent
    steam_cache = load_json_cache(cache_dir / "steam_cache.json")
    rawg_cache = load_json_cache(cache_dir / "rawg_cache.json")
    igdb_cache = load_json_cache(cache_dir / "igdb_cache.json")
    steam_by_id = steam_cache.get("by_id") if isinstance(steam_cache, dict) else {}
    rawg_by_id = rawg_cache.get("by_id") if isinstance(rawg_cache, dict) else {}
    igdb_by_id = igdb_cache.get("by_id") if isinstance(igdb_cache, dict) else {}

    df = load_or_merge_dataframe(input_csv, output_csv) if output_csv else read_csv(input_csv)
    if row_filter:
        from .common import filter_rows_by_ids

        df = filter_rows_by_ids(df, row_filter)
    total_rows = total_named_rows(df)

    processed = 0
    pending_by_id: dict[object, list[int]] = {}

    def _year_hint(row: pd.Series) -> int | None:
        yh = str(row.get("YearHint", "") or "").strip()
        if yh.isdigit() and len(yh) == 4:
            y = int(yh)
            if 1900 <= y <= 2100:
                return y
        return None

    def _derived_year_hint(row: pd.Series) -> int | None:
        yh = _year_hint(row)
        if yh is not None:
            return yh

        rawg_id = str(row.get("RAWG_ID", "") or "").strip()
        if rawg_id and isinstance(rawg_by_id, dict):
            obj = rawg_by_id.get(f"en:{rawg_id}") or rawg_by_id.get(str(rawg_id))
            if isinstance(obj, dict):
                released = str(obj.get("released") or "").strip()
                if len(released) >= 4 and released[:4].isdigit():
                    return int(released[:4])

        igdb_id = str(row.get("IGDB_ID", "") or "").strip()
        if igdb_id and isinstance(igdb_by_id, dict):
            obj = igdb_by_id.get(f"en:{igdb_id}") or igdb_by_id.get(str(igdb_id))
            if isinstance(obj, dict):
                ts = obj.get("first_release_date")
                if isinstance(ts, (int, float)) and ts > 0:
                    try:
                        return int(datetime.fromtimestamp(float(ts)).year)
                    except Exception:
                        pass

        appid = str(row.get("Steam_AppID", "") or "").strip()
        if appid and isinstance(steam_by_id, dict):
            obj = steam_by_id.get(appid)
            if isinstance(obj, dict):
                date_s = str((obj.get("release_date") or {}).get("date") or "").strip()
                import re

                m = re.search(r"\b(19\d{2}|20\d{2})\b", date_s)
                if m:
                    return int(m.group(1))
        return None

    def _fallback_titles(row: pd.Series) -> list[str]:
        titles: list[str] = []
        appid = str(row.get("Steam_AppID", "") or "").strip()
        if appid and isinstance(steam_by_id, dict):
            obj = steam_by_id.get(appid)
            if isinstance(obj, dict):
                t = str(obj.get("name") or "").strip()
                if t:
                    titles.append(t)

        rawg_id = str(row.get("RAWG_ID", "") or "").strip()
        if rawg_id and isinstance(rawg_by_id, dict):
            obj = rawg_by_id.get(f"en:{rawg_id}") or rawg_by_id.get(str(rawg_id))
            if isinstance(obj, dict):
                t = str(obj.get("name") or "").strip()
                if t:
                    titles.append(t)

        igdb_id = str(row.get("IGDB_ID", "") or "").strip()
        if igdb_id and isinstance(igdb_by_id, dict):
            obj = igdb_by_id.get(f"en:{igdb_id}") or igdb_by_id.get(str(igdb_id))
            if isinstance(obj, dict):
                t = str(obj.get("name") or "").strip()
                if t:
                    titles.append(t)

        out: list[str] = []
        seen_set: set[str] = set()
        for t in titles:
            key = t.casefold()
            if key in seen_set:
                continue
            seen_set.add(key)
            out.append(t)
        return out

    wiki_tasks: Queue[tuple[str, str, list[int]] | None] = Queue()
    wiki_results: Queue[tuple[list[int], dict[str, object]]] = Queue()
    wiki_progress = {"enqueued": 0, "done": 0}

    def _drain_wiki_results() -> None:
        while True:
            try:
                indices, fields = wiki_results.get_nowait()
            except Empty:
                return
            for idx2 in indices:
                apply_registered_metrics(
                    df,
                    idx=int(idx2),
                    metrics=fields,
                    registry=registry,
                    label="WIKIPEDIA",
                )

    def _wikipedia_consumer() -> None:
        memo: dict[tuple[str, str], dict[str, object]] = {}
        last_log = time.time()
        while True:
            item = wiki_tasks.get()
            if item is None:
                break
            enwiki_title, release_date, indices = item
            title = str(enwiki_title or "").strip()
            if not title:
                continue
            rel = str(release_date or "").strip()
            key = (title, rel)

            cached = memo.get(key)
            if cached is None:
                pageviews = pageviews_client.get_pageviews_summary_enwiki(title)
                launch = pageviews_client.get_pageviews_launch_summary_enwiki(
                    enwiki_title=title, release_date=rel
                )
                summary = summary_client.get_summary(title)

                fields: dict[str, object] = {}
                if pageviews.days_30 is not None:
                    fields["wikipedia.pageviews_30d"] = pageviews.days_30
                if pageviews.days_90 is not None:
                    fields["wikipedia.pageviews_90d"] = pageviews.days_90
                if pageviews.days_365 is not None:
                    fields["wikipedia.pageviews_365d"] = pageviews.days_365
                if launch and launch.days_30 is not None:
                    fields["wikipedia.pageviews_first_30d"] = launch.days_30
                if launch and launch.days_90 is not None:
                    fields["wikipedia.pageviews_first_90d"] = launch.days_90
                if isinstance(summary, dict) and summary:
                    extract = str(summary.get("extract") or "").strip()
                    if len(extract) > 320:
                        extract = extract[:317].rstrip() + "..."
                    thumb = ""
                    t = summary.get("thumbnail")
                    if isinstance(t, dict):
                        thumb = str(t.get("source") or "").strip()
                    page_url = ""
                    cu = summary.get("content_urls")
                    if isinstance(cu, dict):
                        desktop = cu.get("desktop")
                        if isinstance(desktop, dict):
                            page_url = str(desktop.get("page") or "").strip()
                    fields["wikipedia.summary"] = extract
                    fields["wikipedia.thumbnail"] = thumb
                    fields["wikipedia.page_url"] = page_url

                memo[key] = fields
                cached = fields

            wiki_results.put((indices, cached))
            wiki_progress["done"] += 1
            now = time.time()
            if now - last_log >= float(getattr(CLI, "progress_min_interval_s", 30.0) or 30.0):
                last_log = now
                try:
                    qsize = wiki_tasks.qsize()
                except Exception:
                    qsize = -1
                enq = int(wiki_progress.get("enqueued", 0) or 0)
                done = int(wiki_progress.get("done", 0) or 0)
                total = f"{done}/{enq}" if enq else f"{done}"
                q = f" queue={qsize}" if qsize >= 0 else ""
                logging.info(f"[WIKIPEDIA] Progress {total} tasks{q}")

    def _apply_wikidata_fields(qid: object, indices: list[int], data: object) -> int:
        if not isinstance(data, dict):
            return 0
        enwiki_title = str(data.get("wikidata.enwiki_title") or "").strip()
        release_date = str(data.get("wikidata.release_date") or "").strip()

        for idx2 in indices:
            apply_registered_metrics(df, idx=int(idx2), metrics=data, registry=registry, label="WIKIDATA")
        if enwiki_title:
            wiki_tasks.put((enwiki_title, release_date, list(indices)))
            wiki_progress["enqueued"] += 1
        return len(indices)

    def _flush_pending() -> None:
        nonlocal processed
        processed += flush_pending_keys(
            pending_by_id,
            fetch_many=lambda keys: client.get_by_ids([str(k) for k in keys if str(k).strip()]),
            on_item=_apply_wikidata_fields,
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        wikipedia_future = executor.submit(_wikipedia_consumer)

        for idx, row, _name, seen in iter_named_rows_with_progress(df, label="WIKIDATA", total=total_rows):
            rowid = str(row.get("RowId", "") or "").strip()
            override_qid = ""
            if identity_overrides and rowid:
                override_qid = str(identity_overrides.get(rowid, {}).get("Wikidata_QID", "") or "").strip()

            if override_qid == IDENTITY_NOT_FOUND:
                clear_prefixed_columns(df, int(idx), "Wikidata_")
                continue

            if is_row_processed(df, idx, required_cols):
                if not (override_qid and str(df.at[idx, "Wikidata_QID"] or "").strip() != override_qid):
                    enwiki_title = str(df.at[idx, "Wikidata_EnwikiTitle"] or "").strip()
                    if enwiki_title:
                        wiki_tasks.put(
                            (
                                enwiki_title,
                                str(df.at[idx, "Wikidata_ReleaseDate"] or "").strip(),
                                [int(idx)],
                            )
                        )
                    _drain_wiki_results()
                    continue

            qid = override_qid or str(row.get("Wikidata_QID", "") or "").strip()
            if qid:
                pending_by_id.setdefault(qid, []).append(int(idx))
            else:
                # Enrich is "fetch by pinned ID" only: do not perform name-based Wikidata searches
                # here. Pinning is handled by `import` (and optional `resolve` retries).
                clear_prefixed_columns(df, int(idx), "Wikidata_")
                continue

            if seen % CLI.progress_every_n == 0:
                _drain_wiki_results()
                write_provider_output_csv(df, output_csv, prefix="Wikidata_")

            if len(pending_by_id) >= WIKIDATA.get_by_ids_batch_size:
                _flush_pending()
                _drain_wiki_results()
                write_provider_output_csv(df, output_csv, prefix="Wikidata_")

        _flush_pending()
        wiki_tasks.put(None)
        wikipedia_future.result()
        _drain_wiki_results()

    write_provider_output_csv(df, output_csv, prefix="Wikidata_")
    log_cache_stats(
        {
            "wikidata": client,
            "wikipedia_pageviews": pageviews_client,
            "wikipedia_summary": summary_client,
        }
    )
    logging.info(f"✔ Wikidata completed: {output_csv}")
    return df


def run_enrich(
    *,
    input_csv: Path,
    output_dir: Path,
    cache_dir: Path,
    credentials_path: Path,
    sources: list[str],
    clean_output: bool,
    merge_output: Path,
    validate: bool,
    validate_output: Path | None,
    write_csv: bool = True,
    write_jsonl: bool = True,
    metrics_registry_path: Path | None = None,
    export_json: bool = False,
    all_metrics: bool = False,
    use_catalog_jsonl: bool = True,
    reuse_provider_jsonl: bool = False,
) -> None:
    from .context import PipelineContext

    ctx = PipelineContext(cache_dir=cache_dir, credentials_path=credentials_path, sources=sources)
    run_enrich_ctx(
        ctx,
        input_csv=input_csv,
        output_dir=output_dir,
        clean_output=clean_output,
        merge_output=merge_output,
        validate=validate,
        validate_output=validate_output,
        write_csv=write_csv,
        write_jsonl=write_jsonl,
        metrics_registry_path=metrics_registry_path,
        export_json=export_json,
        all_metrics=all_metrics,
        use_catalog_jsonl=use_catalog_jsonl,
        reuse_provider_jsonl=reuse_provider_jsonl,
    )


def run_enrich_ctx(
    ctx: PipelineContext,
    *,
    input_csv: Path,
    output_dir: Path,
    clean_output: bool,
    merge_output: Path,
    validate: bool,
    validate_output: Path | None,
    write_csv: bool = True,
    write_jsonl: bool = True,
    metrics_registry_path: Path | None = None,
    export_json: bool = False,
    all_metrics: bool = False,
    use_catalog_jsonl: bool = True,
    reuse_provider_jsonl: bool = False,
) -> None:
    if not input_csv.exists():
        raise SystemExit(f"Input file not found: {input_csv}")
    output_dir.mkdir(parents=True, exist_ok=True)
    ctx.cache_dir.mkdir(parents=True, exist_ok=True)

    credentials = load_credentials(ctx.credentials_path)
    if metrics_registry_path is not None:
        registry = load_metrics_registry(metrics_registry_path)
    else:
        registry = load_metrics_registry(default_metrics_registry_path(run_dir=output_dir.parent))

    work_dir = output_dir / "work"
    if not write_csv:
        work_dir.mkdir(parents=True, exist_ok=True)

    def _work_csv(filename: str) -> Path:
        return (output_dir / filename) if write_csv else (work_dir / filename)

    work_provider_igdb = _work_csv("Provider_IGDB.csv")
    work_provider_rawg = _work_csv("Provider_RAWG.csv")
    work_provider_steam = _work_csv("Provider_Steam.csv")
    work_provider_steamspy = _work_csv("Provider_SteamSpy.csv")
    work_provider_hltb = _work_csv("Provider_HLTB.csv")
    work_provider_wikidata = _work_csv("Provider_Wikidata.csv")
    work_merge_output = merge_output if write_csv else (work_dir / "Games_Enriched.csv")

    if clean_output:
        for p in (
            work_provider_igdb,
            work_provider_rawg,
            work_provider_steam,
            work_provider_steamspy,
            work_provider_hltb,
            work_provider_wikidata,
            work_merge_output,
            validate_output or (output_dir / "Validation_Report.csv"),
        ):
            if p.exists():
                p.unlink()

    artifacts = ArtifactStore(
        run_dir=output_dir.parent,
        registry=registry,
        use_jsonl=write_jsonl,
        reuse_jsonl=reuse_provider_jsonl,
        jsonl_dir=output_dir / "jsonl",
    )

    if use_catalog_jsonl:
        base_df = artifacts.load_catalog(input_csv)
    else:
        base_df = read_csv(input_csv)
    diagnostic_columns = set(registry.diagnostic_columns) | {"NeedsReview"}
    base_df = build_personal_base_for_enrich(base_df, diagnostic_columns=diagnostic_columns)
    temp_base_csv = output_dir / f".personal_base.{os.getpid()}.csv"
    write_full_csv(base_df, temp_base_csv)
    input_for_processing = temp_base_csv
    identity_overrides = load_identity_overrides(input_for_processing)

    provider_frames: dict[str, pd.DataFrame] = {}

    def run_source(source: str) -> dict[str, pd.DataFrame]:
        if source == "igdb":
            dfp = artifacts.ensure_provider(
                "igdb",
                compute=lambda: process_igdb(
                    input_csv=input_for_processing,
                    output_csv=work_provider_igdb,
                    cache_path=ctx.cache_dir / "igdb_cache.json",
                    credentials=credentials,
                    required_cols=["IGDB_Name"],
                    registry=registry,
                    identity_overrides=identity_overrides or None,
                ),
                compute_missing=lambda missing: process_igdb(
                    input_csv=input_for_processing,
                    output_csv=work_provider_igdb,
                    cache_path=ctx.cache_dir / "igdb_cache.json",
                    credentials=credentials,
                    required_cols=["IGDB_Name"],
                    registry=registry,
                    identity_overrides=identity_overrides or None,
                    row_filter=missing,
                ),
                expected_row_ids=set(base_df["RowId"].astype(str).str.strip()),
                include_all_metrics=all_metrics,
            )
            return {"igdb": dfp}
        if source == "rawg":
            dfp = artifacts.ensure_provider(
                "rawg",
                compute=lambda: process_rawg(
                    input_csv=input_for_processing,
                    output_csv=work_provider_rawg,
                    cache_path=ctx.cache_dir / "rawg_cache.json",
                    credentials=credentials,
                    required_cols=["RAWG_ID", "RAWG_Year", "RAWG_Genres"],
                    registry=registry,
                    identity_overrides=identity_overrides or None,
                ),
                compute_missing=lambda missing: process_rawg(
                    input_csv=input_for_processing,
                    output_csv=work_provider_rawg,
                    cache_path=ctx.cache_dir / "rawg_cache.json",
                    credentials=credentials,
                    required_cols=["RAWG_ID", "RAWG_Year", "RAWG_Genres"],
                    registry=registry,
                    identity_overrides=identity_overrides or None,
                    row_filter=missing,
                ),
                expected_row_ids=set(base_df["RowId"].astype(str).str.strip()),
                include_all_metrics=all_metrics,
            )
            return {"rawg": dfp}
        if source == "steam":
            dfp = artifacts.ensure_provider(
                "steam",
                compute=lambda: process_steam(
                    input_csv=input_for_processing,
                    output_csv=work_provider_steam,
                    cache_path=ctx.cache_dir / "steam_cache.json",
                    required_cols=["Steam_Name"],
                    registry=registry,
                    identity_overrides=identity_overrides or None,
                ),
                compute_missing=lambda missing: process_steam(
                    input_csv=input_for_processing,
                    output_csv=work_provider_steam,
                    cache_path=ctx.cache_dir / "steam_cache.json",
                    required_cols=["Steam_Name"],
                    registry=registry,
                    identity_overrides=identity_overrides or None,
                    row_filter=missing,
                ),
                expected_row_ids=set(base_df["RowId"].astype(str).str.strip()),
                include_all_metrics=all_metrics,
            )
            return {"steam": dfp}
        if source == "steamspy":
            dfp = artifacts.ensure_provider(
                "steamspy",
                compute=lambda: process_steamspy(
                    input_csv=work_provider_steam,
                    output_csv=work_provider_steamspy,
                    cache_path=ctx.cache_dir / "steamspy_cache.json",
                    required_cols=["SteamSpy_Owners"],
                    registry=registry,
                ),
                compute_missing=lambda missing: process_steamspy(
                    input_csv=work_provider_steam,
                    output_csv=work_provider_steamspy,
                    cache_path=ctx.cache_dir / "steamspy_cache.json",
                    required_cols=["SteamSpy_Owners"],
                    registry=registry,
                    row_filter=missing,
                ),
                expected_row_ids=set(base_df["RowId"].astype(str).str.strip()),
                include_all_metrics=all_metrics,
            )
            return {"steamspy": dfp}
        if source == "steam+steamspy":
            reused_steam = artifacts.load_provider_jsonl("steam")
            reused_spy = artifacts.load_provider_jsonl("steamspy")
            if reused_steam is not None and reused_spy is not None:
                return {"steam": reused_steam, "steamspy": reused_spy}
            df_steam, df_spy = process_steam_and_steamspy_streaming(
                input_csv=input_for_processing,
                steam_output_csv=work_provider_steam,
                steamspy_output_csv=work_provider_steamspy,
                steam_cache_path=ctx.cache_dir / "steam_cache.json",
                steamspy_cache_path=ctx.cache_dir / "steamspy_cache.json",
                registry=registry,
                identity_overrides=identity_overrides or None,
            )
            if artifacts.use_jsonl:
                artifacts.ensure_provider(
                    "steam",
                    compute=lambda: df_steam,
                    compute_missing=None,
                    expected_row_ids=set(base_df["RowId"].astype(str).str.strip()),
                    include_all_metrics=all_metrics,
                )
                artifacts.ensure_provider(
                    "steamspy",
                    compute=lambda: df_spy,
                    compute_missing=None,
                    expected_row_ids=set(base_df["RowId"].astype(str).str.strip()),
                    include_all_metrics=all_metrics,
                )
            return {"steam": df_steam, "steamspy": df_spy}
        if source == "hltb":
            dfp = artifacts.ensure_provider(
                "hltb",
                compute=lambda: process_hltb(
                    input_csv=input_for_processing,
                    output_csv=work_provider_hltb,
                    cache_path=ctx.cache_dir / "hltb_cache.json",
                    required_cols=["HLTB_Main"],
                    registry=registry,
                    identity_overrides=identity_overrides or None,
                ),
                compute_missing=lambda missing: process_hltb(
                    input_csv=input_for_processing,
                    output_csv=work_provider_hltb,
                    cache_path=ctx.cache_dir / "hltb_cache.json",
                    required_cols=["HLTB_Main"],
                    registry=registry,
                    identity_overrides=identity_overrides or None,
                    row_filter=missing,
                ),
                expected_row_ids=set(base_df["RowId"].astype(str).str.strip()),
                include_all_metrics=all_metrics,
            )
            return {"hltb": dfp}
        if source == "wikidata":
            dfp = artifacts.ensure_provider(
                "wikidata",
                compute=lambda: process_wikidata(
                    input_csv=input_for_processing,
                    output_csv=work_provider_wikidata,
                    cache_path=ctx.cache_dir / "wikidata_cache.json",
                    required_cols=["Wikidata_Label"],
                    registry=registry,
                    identity_overrides=identity_overrides or None,
                ),
                compute_missing=lambda missing: process_wikidata(
                    input_csv=input_for_processing,
                    output_csv=work_provider_wikidata,
                    cache_path=ctx.cache_dir / "wikidata_cache.json",
                    required_cols=["Wikidata_Label"],
                    registry=registry,
                    identity_overrides=identity_overrides or None,
                    row_filter=missing,
                ),
                expected_row_ids=set(base_df["RowId"].astype(str).str.strip()),
                include_all_metrics=all_metrics,
            )
            return {"wikidata": dfp}
        raise ValueError(f"Unknown source: {source}")

    # Run providers in parallel when we have multiple independent sources.
    sources_to_process = list(ctx.sources)
    if "steam" in sources_to_process and "steamspy" in sources_to_process:
        sources_to_process = [s for s in sources_to_process if s not in ("steam", "steamspy")] + [
            "steam+steamspy"
        ]

    if len(sources_to_process) <= 1:
        provider_frames.update(run_source(sources_to_process[0]))
    else:
        max_workers = min(len(sources_to_process), int(getattr(CLI, "max_parallel_providers", 8) or 8))
        max_workers = max(1, max_workers)
        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for src in sources_to_process:
                futures[executor.submit(run_source, src)] = src
            errors: list[tuple[str, BaseException]] = []
            for future in as_completed(futures):
                src = futures[future]
                try:
                    provider_frames.update(future.result())
                except BaseException as e:
                    errors.append((src, e))
            if errors:
                src, err = errors[0]
                raise RuntimeError(f"Provider '{src}' failed") from err

    enabled = {s.strip().lower() for s in ctx.sources if s.strip()}
    merged_df = base_df.copy()

    def _is_present(v: object) -> bool:
        if v is None:
            return False
        if isinstance(v, (list, dict)):
            return bool(v)
        return str(v or "").strip() != ""

    def _overlay_merge(base: pd.DataFrame, other: pd.DataFrame, *, suffix: str) -> pd.DataFrame:
        merged = base.merge(other, on="RowId", how="left", suffixes=("", suffix))
        for c in list(merged.columns):
            if not c.endswith(suffix):
                continue
            orig = c[: -len(suffix)]
            if orig in merged.columns:
                mask = merged[c].map(_is_present)
                merged.loc[mask, orig] = merged.loc[mask, c]
            merged = merged.drop(columns=[c])
        return merged

    def _provider_overlay(prefix: str, prov: str) -> None:
        nonlocal merged_df
        dfp = provider_frames.get(prov)
        if dfp is None:
            return
        cols = ["RowId"] + [c for c in dfp.columns if c.startswith(prefix)]
        merged_df = _overlay_merge(merged_df, cast(pd.DataFrame, dfp[cols]), suffix=f"__{prov}")

    if "rawg" in enabled:
        _provider_overlay("RAWG_", "rawg")
    if "igdb" in enabled:
        _provider_overlay("IGDB_", "igdb")
    if "steam" in enabled:
        _provider_overlay("Steam_", "steam")
    if "steamspy" in enabled:
        _provider_overlay("SteamSpy_", "steamspy")
    if "hltb" in enabled:
        _provider_overlay("HLTB_", "hltb")
    if "wikidata" in enabled:
        _provider_overlay("Wikidata_", "wikidata")

    merged_df = apply_phase1_signals(merged_df, registry=registry)
    merged_df = reorder_columns(merged_df, registry=registry)
    merged_df = drop_eval_columns(merged_df, diagnostic_columns=diagnostic_columns)
    deprecated_cols = [
        "IGDB_Companies",
        "Steam_ReviewPercent",
        "SteamSpy_Players",
        "SteamSpy_Players2Weeks",
        "Now_SteamSpyPlayers2Weeks",
    ]
    merged_df = merged_df.drop(columns=[c for c in deprecated_cols if c in merged_df.columns])
    write_full_csv(merged_df, work_merge_output)
    logging.info(f"✔ Games_Enriched.csv generated successfully: {work_merge_output}")

    # Internal JSONL outputs: provider + merged JSONL alongside CSV.
    if write_jsonl and registry is not None:
        artifacts.write_enriched(
            merged_df,
            provider_frames=provider_frames,
            include_all_metrics=all_metrics,
            export_json=export_json,
            merge_output=merge_output if write_csv else None,
            metrics_registry_path=metrics_registry_path,
        )

    if validate:
        validate_out = validate_output or (output_dir / "Validation_Report.csv")
        merged = merged_df
        enabled_for_validation = {s.strip().lower() for s in ctx.sources if s.strip()}

        def _has_any(col: str) -> bool:
            if col not in merged.columns:
                return False
            return bool(merged[col].astype(str).str.strip().ne("").any())

        if _has_any("RAWG_ID"):
            enabled_for_validation.add("rawg")
        if _has_any("IGDB_ID"):
            enabled_for_validation.add("igdb")
        if _has_any("Steam_AppID"):
            enabled_for_validation.add("steam")
        if any(c.startswith("SteamSpy_") for c in merged.columns) and _has_any("SteamSpy_Owners"):
            enabled_for_validation.add("steamspy")
        if _has_any("HLTB_Main"):
            enabled_for_validation.add("hltb")
        if _has_any("Wikidata_QID"):
            enabled_for_validation.add("wikidata")

        report = generate_validation_report(merged, enabled_providers=enabled_for_validation)
        write_full_csv(report, validate_out)
        logging.info(f"✔ Validation report generated: {validate_out}")

    # If JSONL is disabled but CSV outputs are requested and a registry is available, apply
    # the registry's selection directly to the written CSVs.
    if write_csv and (not write_jsonl) and registry is not None:
        enabled = {s.strip().lower() for s in ctx.sources if s.strip()}

        def _is_metric_candidate(col: str) -> bool:
            c = str(col or "").strip()
            if not c:
                return False
            return c.startswith(PROVIDER_PREFIXES) or c in registry.metric_columns

        def _filter_csv(path: Path) -> None:
            if not path.exists():
                return
            df = read_csv(path)
            cols = list(df.columns)
            keep: list[str] = []
            for c in cols:
                if c == "RowId":
                    keep.append(c)
                    continue
                if _is_metric_candidate(c) and c not in registry.metric_columns:
                    continue
                keep.append(c)
            write_full_csv(cast(pd.DataFrame, df[keep]), path)

        _filter_csv(merge_output)
        for prov, p in (
            ("rawg", output_dir / "Provider_RAWG.csv"),
            ("igdb", output_dir / "Provider_IGDB.csv"),
            ("steam", output_dir / "Provider_Steam.csv"),
            ("steamspy", output_dir / "Provider_SteamSpy.csv"),
            ("hltb", output_dir / "Provider_HLTB.csv"),
            ("wikidata", output_dir / "Provider_Wikidata.csv"),
        ):
            if prov not in enabled:
                continue
            _filter_csv(p)

    if not write_csv:
        if work_dir.exists():
            for p in work_dir.glob("*"):
                try:
                    p.unlink()
                except Exception:
                    pass
            try:
                work_dir.rmdir()
            except Exception:
                pass

    if temp_base_csv.exists():
        temp_base_csv.unlink()
