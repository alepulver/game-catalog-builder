from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import TYPE_CHECKING

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
from ..schema import EVAL_COLUMNS, PUBLIC_DEFAULT_COLS, provider_output_columns
from ..utils import (
    IDENTITY_NOT_FOUND,
    ensure_columns,
    generate_validation_report,
    is_row_processed,
    load_credentials,
    load_identity_overrides,
    load_json_cache,
    merge_all,
    normalize_game_name,
    read_csv,
    write_csv,
)
from ..utils.progress import Progress

if TYPE_CHECKING:
    from .context import PipelineContext


def drop_eval_columns(df: pd.DataFrame) -> pd.DataFrame:
    preserve = {"Steam_StoreType"}
    cols = [c for c in EVAL_COLUMNS if c in df.columns and c not in preserve]
    return df.drop(columns=cols) if cols else df


def clear_prefixed_columns(df: pd.DataFrame, idx: int, prefix: str) -> None:
    for c in [col for col in df.columns if col.startswith(prefix)]:
        df.at[idx, c] = ""


def load_or_merge_dataframe(input_csv: Path, output_csv: Path) -> pd.DataFrame:
    """
    Load dataframe from input CSV, merging in existing data from output CSV if it exists.

    This ensures we always process all games from the input, while preserving already-processed
    data from previous runs.
    """
    df = read_csv(input_csv)
    if "RowId" not in df.columns:
        raise SystemExit(f"Missing RowId in {input_csv}; run `import` first.")

    if output_csv.exists():
        df_output = read_csv(output_csv)
        if "RowId" not in df_output.columns:
            raise SystemExit(
                f"Missing RowId in {output_csv}; delete it and re-run, or regenerate outputs."
            )
        df = df.merge(df_output, on="RowId", how="left", suffixes=("", "_existing"))
        for col in list(df.columns):
            if not col.endswith("_existing"):
                continue
            original_col = col.replace("_existing", "")
            if original_col in df.columns:
                mask = (df[col].notna()) & (df[col] != "")
                df.loc[mask, original_col] = df.loc[mask, col]
            df = df.drop(columns=[col])

    df = ensure_columns(df, PUBLIC_DEFAULT_COLS)
    return df


def build_personal_base_for_enrich(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the base dataframe for a fresh merge by removing provider-derived columns.

    This is critical for "in-place" enrich (input == output) to avoid keeping stale provider
    columns: merge_all is a left-join and does not overwrite existing same-named columns.
    """
    from ..schema import EVAL_COLUMNS, PINNED_ID_COLS

    drop_prefixes = ("RAWG_", "IGDB_", "Steam_", "SteamSpy_", "HLTB_", "Wikidata_")
    keep = set(PINNED_ID_COLS) | {"RowId", "Name"}
    drop_eval = set(EVAL_COLUMNS)

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
    return df[cols].copy()


def _total_named_rows(df: pd.DataFrame) -> int:
    if "Name" not in df.columns:
        return 0
    return int((df["Name"].astype(str).str.strip() != "").sum())


def process_steam_and_steamspy_streaming(
    *,
    input_csv: Path,
    steam_output_csv: Path,
    steamspy_output_csv: Path,
    steam_cache_path: Path,
    steamspy_cache_path: Path,
    identity_overrides: dict[str, dict[str, str]] | None = None,
) -> None:
    steam_client = SteamClient(cache_path=steam_cache_path, min_interval_s=STEAM.storesearch_min_interval_s)
    steamspy_client = SteamSpyClient(cache_path=steamspy_cache_path, min_interval_s=STEAMSPY.min_interval_s)

    df_steam = load_or_merge_dataframe(input_csv, steam_output_csv)
    df_steamspy = read_csv(input_csv)
    df_steamspy = ensure_columns(df_steamspy, PUBLIC_DEFAULT_COLS)
    total_steam_rows = _total_named_rows(df_steam)
    q: Queue[tuple[int, str, str] | None] = Queue()

    def steam_producer() -> None:
        processed = 0
        seen = 0
        progress = Progress("STEAM", total=total_steam_rows or None, every_n=CLI.progress_every_n)
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
            seen += 1
            progress.maybe_log(seen)

            rowid = str(row.get("RowId", "") or "").strip()
            override_appid = ""
            if identity_overrides and rowid:
                override_appid = str(
                    identity_overrides.get(rowid, {}).get("Steam_AppID", "") or ""
                ).strip()

            if override_appid == IDENTITY_NOT_FOUND:
                clear_prefixed_columns(df_steam, int(idx), "Steam_")
                continue

            if is_row_processed(df_steam, int(idx), ["Steam_Name"]):
                current_appid = str(df_steam.at[idx, "Steam_AppID"] or "").strip()
                if current_appid and not is_row_processed(df_steamspy, int(idx), ["SteamSpy_Owners"]):
                    q.put((int(idx), name, current_appid))
                if not (override_appid and current_appid != override_appid):
                    continue

            if override_appid:
                appid = override_appid
            else:
                logging.debug(f"[STEAM] Processing: {name}")
                search = steam_client.search_appid(name)
                if not search:
                    continue
                appid = str(search.get("id") or "").strip()
                if not appid:
                    continue

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
                write_csv(df_steam[provider_output_columns(list(df_steam.columns), prefix="Steam_")], steam_output_csv)

            if len(pending) >= CLI.steam_streaming_flush_batch_size:
                _flush_pending()
                write_csv(df_steam[provider_output_columns(list(df_steam.columns), prefix="Steam_")], steam_output_csv)

        _flush_pending()
        write_csv(df_steam[provider_output_columns(list(df_steam.columns), prefix="Steam_")], steam_output_csv)
        q.put(None)

    def steamspy_consumer() -> None:
        processed = 0
        progress = Progress("STEAMSPY", total=None, every_n=CLI.progress_every_n)
        while True:
            item = q.get()
            if item is None:
                break
            idx, name, appid = item
            if is_row_processed(df_steamspy, idx, ["SteamSpy_Owners"]):
                continue
            logging.debug(f"[STEAMSPY] {name} (AppID {appid})")
            data = steamspy_client.fetch(int(appid))
            if not data:
                continue
            for k, v in data.items():
                df_steamspy.at[idx, k] = v
            processed += 1
            progress.maybe_log(processed)
            if processed % 10 == 0:
                steamspy_cols = provider_output_columns(
                    list(df_steamspy.columns), prefix="SteamSpy_", extra=("Score_SteamSpy_100",)
                )
                write_csv(df_steamspy[steamspy_cols], steamspy_output_csv)

        steamspy_cols = provider_output_columns(
            list(df_steamspy.columns), prefix="SteamSpy_", extra=("Score_SteamSpy_100",)
        )
        write_csv(df_steamspy[steamspy_cols], steamspy_output_csv)

    with ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(steam_producer)
        f2 = executor.submit(steamspy_consumer)
        f1.result()
        f2.result()

    logging.info(f"[STEAM] Cache stats: {steam_client.format_cache_stats()}")
    logging.info(f"[STEAMSPY] Cache stats: {steamspy_client.format_cache_stats()}")


def process_igdb(
    *,
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    credentials: dict,
    required_cols: list[str],
    language: str = "en",
    identity_overrides: dict[str, dict[str, str]] | None = None,
) -> None:
    client = IGDBClient(
        client_id=credentials.get("igdb", {}).get("client_id", ""),
        client_secret=credentials.get("igdb", {}).get("client_secret", ""),
        cache_path=cache_path,
        language=language,
        min_interval_s=IGDB.min_interval_s,
    )
    df = load_or_merge_dataframe(input_csv, output_csv)
    total_rows = _total_named_rows(df)
    progress = Progress("IGDB", total=total_rows or None, every_n=CLI.progress_every_n)

    processed = 0
    seen = 0
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
        name = str(row.get("Name", "") or "").strip()
        if not name:
            continue
        seen += 1
        progress.maybe_log(seen)

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
                for k, v in data.items():
                    df.at[idx, k] = v
                processed += 1

        if processed % 10 == 0:
            cols = provider_output_columns(
                list(df.columns), prefix="IGDB_", extra=("Score_IGDB_100", "Score_IGDBCritic_100")
            )
            write_csv(df[cols], output_csv)

        if len(pending_by_id) >= CLI.igdb_flush_batch_size:
            _flush_pending()
            cols = provider_output_columns(
                list(df.columns), prefix="IGDB_", extra=("Score_IGDB_100", "Score_IGDBCritic_100")
            )
            write_csv(df[cols], output_csv)

    _flush_pending()
    cols = provider_output_columns(
        list(df.columns), prefix="IGDB_", extra=("Score_IGDB_100", "Score_IGDBCritic_100")
    )
    write_csv(df[cols], output_csv)
    logging.info(f"[IGDB] Cache stats: {client.format_cache_stats()}")
    logging.info(f"✔ IGDB completed: {output_csv}")


def process_rawg(
    *,
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    credentials: dict,
    required_cols: list[str],
    language: str = "en",
    identity_overrides: dict[str, dict[str, str]] | None = None,
) -> None:
    client = RAWGClient(
        api_key=credentials.get("rawg", {}).get("api_key", ""),
        cache_path=cache_path,
        language=language,
        min_interval_s=RAWG.min_interval_s,
    )
    df = load_or_merge_dataframe(input_csv, output_csv)
    total_rows = _total_named_rows(df)
    progress = Progress("RAWG", total=total_rows or None, every_n=CLI.progress_every_n)

    processed = 0
    seen = 0
    for idx, row in df.iterrows():
        name = str(row.get("Name", "") or "").strip()
        if not name:
            continue
        seen += 1
        progress.maybe_log(seen)

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

        fields = client.extract_fields(result)
        for k, v in fields.items():
            df.at[idx, k] = v

        processed += 1
        if processed % 10 == 0:
            cols = provider_output_columns(list(df.columns), prefix="RAWG_", extra=("Score_RAWG_100",))
            write_csv(df[cols], output_csv)

    cols = provider_output_columns(list(df.columns), prefix="RAWG_", extra=("Score_RAWG_100",))
    write_csv(df[cols], output_csv)
    logging.info(f"[RAWG] Cache stats: {client.format_cache_stats()}")
    logging.info(f"✔ RAWG completed: {output_csv}")


def process_steam(
    *,
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    required_cols: list[str],
    identity_overrides: dict[str, dict[str, str]] | None = None,
) -> None:
    client = SteamClient(cache_path=cache_path, min_interval_s=STEAM.storesearch_min_interval_s)
    df = load_or_merge_dataframe(input_csv, output_csv)
    total_rows = _total_named_rows(df)
    progress = Progress("STEAM", total=total_rows or None, every_n=CLI.progress_every_n)

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
    seen = 0
    for idx, row in df.iterrows():
        name = str(row.get("Name", "") or "").strip()
        if not name:
            continue
        seen += 1
        progress.maybe_log(seen)

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

        if queued % 10 == 0:
            write_csv(df[provider_output_columns(list(df.columns), prefix="Steam_")], output_csv)

        if len(pending) >= CLI.steam_flush_batch_size:
            _flush_pending()
            write_csv(df[provider_output_columns(list(df.columns), prefix="Steam_")], output_csv)

    _flush_pending()
    write_csv(df[provider_output_columns(list(df.columns), prefix="Steam_")], output_csv)
    logging.info(f"[STEAM] Cache stats: {client.format_cache_stats()}")
    logging.info(f"✔ Steam completed: {output_csv}")


def process_steamspy(
    *,
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    required_cols: list[str],
) -> None:
    client = SteamSpyClient(cache_path=cache_path, min_interval_s=STEAMSPY.min_interval_s)
    if not input_csv.exists():
        raise FileNotFoundError(f"{input_csv} not found. Run steam processing first.")
    df = load_or_merge_dataframe(input_csv, output_csv)
    total_rows = int((df.get("Steam_AppID", "").astype(str).str.strip() != "").sum())
    progress = Progress("STEAMSPY", total=total_rows or None, every_n=CLI.progress_every_n)

    processed = 0
    seen = 0
    for idx, row in df.iterrows():
        appid = str(row.get("Steam_AppID", "") or "").strip()
        if not appid:
            continue
        name = str(row.get("Name", "") or "").strip()
        seen += 1
        progress.maybe_log(seen)
        if is_row_processed(df, idx, required_cols):
            continue
        logging.debug(f"[STEAMSPY] {name} (AppID {appid})")
        data = client.fetch(int(appid))
        if not data:
            logging.warning(f"  ↳ No data in SteamSpy: {name} (AppID {appid})")
            continue
        for k, v in data.items():
            df.at[idx, k] = v
        processed += 1
        if processed % 10 == 0:
            cols = provider_output_columns(
                list(df.columns), prefix="SteamSpy_", extra=("Score_SteamSpy_100",)
            )
            write_csv(df[cols], output_csv)

    cols = provider_output_columns(list(df.columns), prefix="SteamSpy_", extra=("Score_SteamSpy_100",))
    write_csv(df[cols], output_csv)
    logging.info(f"[STEAMSPY] Cache stats: {client.format_cache_stats()}")
    logging.info(f"✔ SteamSpy completed: {output_csv}")


def process_hltb(
    *,
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    required_cols: list[str],
    identity_overrides: dict[str, dict[str, str]] | None = None,
) -> None:
    client = HLTBClient(cache_path=cache_path)
    df = load_or_merge_dataframe(input_csv, output_csv)
    total_rows = _total_named_rows(df)
    progress = Progress("HLTB", total=total_rows or None, every_n=CLI.progress_every_n)

    processed = 0
    seen = 0
    for idx, row in df.iterrows():
        name = str(row.get("Name", "") or "").strip()
        if not name:
            continue
        seen += 1
        progress.maybe_log(seen)

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

        logging.debug(f"[HLTB] Processing: {query}")
        data = client.search(name, query=query, hltb_id=pinned_id or None)
        if not data:
            continue
        for k, v in data.items():
            df.at[idx, k] = v

        processed += 1
        if processed % 10 == 0:
            cols = provider_output_columns(list(df.columns), prefix="HLTB_", extra=("Score_HLTB_100",))
            write_csv(df[cols], output_csv)

    cols = provider_output_columns(list(df.columns), prefix="HLTB_", extra=("Score_HLTB_100",))
    write_csv(df[cols], output_csv)
    logging.info(f"[HLTB] Cache stats: {client.format_cache_stats()}")
    logging.info(f"✔ HLTB completed: {output_csv}")


def process_wikidata(
    *,
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    required_cols: list[str],
    identity_overrides: dict[str, dict[str, str]] | None = None,
) -> None:
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

    df = load_or_merge_dataframe(input_csv, output_csv)
    total_rows = _total_named_rows(df)
    progress = Progress("WIKIDATA", total=total_rows or None, every_n=CLI.progress_every_n)

    processed = 0
    seen = 0
    pending_by_id: dict[str, list[int]] = {}

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

    def _flush_pending() -> None:
        nonlocal processed
        if not pending_by_id:
            return
        qids = list(pending_by_id.keys())
        by_qid = client.get_by_ids(qids)
        for qid, indices in list(pending_by_id.items()):
            data = by_qid.get(qid)
            if not data:
                continue
            enwiki_title = str(data.get("Wikidata_EnwikiTitle") or "").strip()
            pageviews = None
            if enwiki_title:
                pageviews = pageviews_client.get_pageviews_summary_enwiki(enwiki_title)
                launch = pageviews_client.get_pageviews_launch_summary_enwiki(
                    enwiki_title=enwiki_title, release_date=str(data.get("Wikidata_ReleaseDate") or "")
                )
                summary = summary_client.get_summary(enwiki_title)
            else:
                launch = None
                summary = None

            for idx2 in indices:
                for k, v in data.items():
                    df.at[idx2, k] = v
                if pageviews is not None:
                    df.at[idx2, "Wikidata_Pageviews30d"] = (
                        str(pageviews.days_30) if pageviews.days_30 is not None else ""
                    )
                    df.at[idx2, "Wikidata_Pageviews90d"] = (
                        str(pageviews.days_90) if pageviews.days_90 is not None else ""
                    )
                    df.at[idx2, "Wikidata_Pageviews365d"] = (
                        str(pageviews.days_365) if pageviews.days_365 is not None else ""
                    )
                df.at[idx2, "Wikidata_PageviewsFirst30d"] = (
                    str(launch.days_30) if launch and launch.days_30 is not None else ""
                )
                df.at[idx2, "Wikidata_PageviewsFirst90d"] = (
                    str(launch.days_90) if launch and launch.days_90 is not None else ""
                )
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
                    df.at[idx2, "Wikidata_WikipediaSummary"] = extract
                    df.at[idx2, "Wikidata_WikipediaThumbnail"] = thumb
                    df.at[idx2, "Wikidata_WikipediaPage"] = page_url
            processed += len(indices)
        pending_by_id.clear()

    for idx, row in df.iterrows():
        seen += 1
        name = str(row.get("Name", "") or "").strip()
        if not name:
            continue
        progress.maybe_log(seen)

        rowid = str(row.get("RowId", "") or "").strip()
        override_qid = ""
        if identity_overrides and rowid:
            override_qid = str(identity_overrides.get(rowid, {}).get("Wikidata_QID", "") or "").strip()

        if override_qid == IDENTITY_NOT_FOUND:
            clear_prefixed_columns(df, int(idx), "Wikidata_")
            continue

        if is_row_processed(df, idx, required_cols):
            if not (override_qid and str(df.at[idx, "Wikidata_QID"] or "").strip() != override_qid):
                continue

        qid = override_qid or str(row.get("Wikidata_QID", "") or "").strip()
        if qid:
            pending_by_id.setdefault(qid, []).append(int(idx))
        else:
            logging.debug(f"[WIKIDATA] Processing: {name}")
            yh = _derived_year_hint(row)
            search = client.search(name, year_hint=yh)
            if not search:
                for alt in _fallback_titles(row):
                    if alt.casefold() == name.casefold():
                        continue
                    search = client.search(alt, year_hint=yh)
                    if search:
                        break
            qid = str((search or {}).get("Wikidata_QID") or "").strip()
            if not qid:
                continue
            df.at[idx, "Wikidata_QID"] = qid
            pending_by_id.setdefault(qid, []).append(int(idx))

        if seen % CLI.progress_every_n == 0:
            cols = provider_output_columns(list(df.columns), prefix="Wikidata_")
            write_csv(df[cols], output_csv)

        if len(pending_by_id) >= WIKIDATA.get_by_ids_batch_size:
            _flush_pending()
            cols = provider_output_columns(list(df.columns), prefix="Wikidata_")
            write_csv(df[cols], output_csv)

    _flush_pending()
    cols = provider_output_columns(list(df.columns), prefix="Wikidata_")
    write_csv(df[cols], output_csv)
    logging.info(f"[WIKIDATA] Cache stats: {client.format_cache_stats()}")
    logging.info(f"[WIKIPEDIA] Cache stats: {pageviews_client.format_cache_stats()}")
    logging.info(f"[WIKIPEDIA] Summary cache stats: {summary_client.format_cache_stats()}")
    logging.info(f"✔ Wikidata completed: {output_csv}")


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
) -> None:
    if not input_csv.exists():
        raise SystemExit(f"Input file not found: {input_csv}")
    output_dir.mkdir(parents=True, exist_ok=True)
    ctx.cache_dir.mkdir(parents=True, exist_ok=True)

    credentials = load_credentials(ctx.credentials_path)

    if clean_output:
        for p in (
            output_dir / "Provider_IGDB.csv",
            output_dir / "Provider_RAWG.csv",
            output_dir / "Provider_Steam.csv",
            output_dir / "Provider_SteamSpy.csv",
            output_dir / "Provider_HLTB.csv",
            output_dir / "Provider_Wikidata.csv",
            merge_output,
            validate_output or (output_dir / "Validation_Report.csv"),
        ):
            if p.exists():
                p.unlink()

    base_df = build_personal_base_for_enrich(read_csv(input_csv))
    temp_base_csv = output_dir / f".personal_base.{os.getpid()}.csv"
    write_csv(base_df, temp_base_csv)
    input_for_processing = temp_base_csv
    identity_overrides = load_identity_overrides(input_for_processing)

    def run_source(source: str) -> None:
        if source == "igdb":
            process_igdb(
                input_csv=input_for_processing,
                output_csv=output_dir / "Provider_IGDB.csv",
                cache_path=ctx.cache_dir / "igdb_cache.json",
                credentials=credentials,
                required_cols=["IGDB_Name"],
                identity_overrides=identity_overrides or None,
            )
            return
        if source == "rawg":
            process_rawg(
                input_csv=input_for_processing,
                output_csv=output_dir / "Provider_RAWG.csv",
                cache_path=ctx.cache_dir / "rawg_cache.json",
                credentials=credentials,
                required_cols=["RAWG_ID", "RAWG_Year", "RAWG_Genre"],
                identity_overrides=identity_overrides or None,
            )
            return
        if source == "steam":
            process_steam(
                input_csv=input_for_processing,
                output_csv=output_dir / "Provider_Steam.csv",
                cache_path=ctx.cache_dir / "steam_cache.json",
                required_cols=["Steam_Name"],
                identity_overrides=identity_overrides or None,
            )
            return
        if source == "steamspy":
            process_steamspy(
                input_csv=output_dir / "Provider_Steam.csv",
                output_csv=output_dir / "Provider_SteamSpy.csv",
                cache_path=ctx.cache_dir / "steamspy_cache.json",
                required_cols=["SteamSpy_Owners"],
            )
            return
        if source == "steam+steamspy":
            process_steam_and_steamspy_streaming(
                input_csv=input_for_processing,
                steam_output_csv=output_dir / "Provider_Steam.csv",
                steamspy_output_csv=output_dir / "Provider_SteamSpy.csv",
                steam_cache_path=ctx.cache_dir / "steam_cache.json",
                steamspy_cache_path=ctx.cache_dir / "steamspy_cache.json",
                identity_overrides=identity_overrides or None,
            )
            return
        if source == "hltb":
            process_hltb(
                input_csv=input_for_processing,
                output_csv=output_dir / "Provider_HLTB.csv",
                cache_path=ctx.cache_dir / "hltb_cache.json",
                required_cols=["HLTB_Main"],
                identity_overrides=identity_overrides or None,
            )
            return
        if source == "wikidata":
            process_wikidata(
                input_csv=input_for_processing,
                output_csv=output_dir / "Provider_Wikidata.csv",
                cache_path=ctx.cache_dir / "wikidata_cache.json",
                required_cols=["Wikidata_Label"],
                identity_overrides=identity_overrides or None,
            )
            return
        raise ValueError(f"Unknown source: {source}")

    # Run providers in parallel when we have multiple independent sources.
    sources_to_process = list(ctx.sources)
    if "steam" in sources_to_process and "steamspy" in sources_to_process:
        sources_to_process = [s for s in sources_to_process if s not in ("steam", "steamspy")] + [
            "steam+steamspy"
        ]

    if len(sources_to_process) <= 1:
        run_source(sources_to_process[0])
    else:
        max_workers = min(len(sources_to_process), (os.cpu_count() or 4))
        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for src in sources_to_process:
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
        wikidata_csv=output_dir / "Provider_Wikidata.csv",
    )
    merged_df = read_csv(merge_output)
    merged_df = drop_eval_columns(merged_df)
    deprecated_cols = [
        "IGDB_Companies",
        "Steam_ReviewPercent",
        "SteamSpy_Players",
        "SteamSpy_Players2Weeks",
        "Now_SteamSpyPlayers2Weeks",
    ]
    merged_df = merged_df.drop(columns=[c for c in deprecated_cols if c in merged_df.columns])
    write_csv(merged_df, merge_output)
    logging.info(f"✔ Games_Enriched.csv generated successfully: {merge_output}")

    if validate:
        validate_out = validate_output or (output_dir / "Validation_Report.csv")
        merged = read_csv(merge_output)
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
        write_csv(report, validate_out)
        logging.info(f"✔ Validation report generated: {validate_out}")

    if temp_base_csv.exists():
        temp_base_csv.unlink()
