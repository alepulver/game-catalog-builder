"""Command-line interface for game catalog builder."""

from __future__ import annotations

import argparse
import logging
import os
import re
import shlex
import sys
import time
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
    WikidataClient,
    WikipediaPageviewsClient,
    WikipediaSummaryClient,
)
from .config import CLI, IGDB, RAWG, STEAM, STEAMSPY, WIKIDATA
from .diagnostics.import_diagnostics import (
    fill_eval_tags,
    platform_is_pc_like as _platform_is_pc_like,
)
from .schema import DIAGNOSTIC_COLUMNS, EVAL_COLUMNS, PINNED_ID_COLS, PROVIDER_PREFIXES
from .utils import (
    IDENTITY_NOT_FOUND,
    PUBLIC_DEFAULT_COLS,
    ProjectPaths,
    ReviewConfig,
    build_review_csv,
    ensure_columns,
    ensure_row_ids,
    extract_year_hint,
    fuzzy_score,
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
from .utils.consistency import (
    actionable_mismatch_tags,
    compute_provider_consensus,
    compute_year_consensus,
    platform_outlier_tags,
    year_outlier_tags,
)
from .diagnostics.resolve import resolve_catalog_pins


def clear_prefixed_columns(df: pd.DataFrame, idx: int, prefix: str) -> None:
    for c in [col for col in df.columns if col.startswith(prefix)]:
        df.at[idx, c] = ""


def drop_eval_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in EVAL_COLUMNS if c in df.columns]
    return df.drop(columns=cols) if cols else df


def build_personal_base_for_enrich(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the base dataframe for a fresh merge by removing provider-derived columns.

    This is critical for "in-place" enrich (input == output) to avoid keeping stale provider
    columns: merge_all is a left-join and does not overwrite existing same-named columns.
    """
    derived_prefixes = (
        "Reach_",
        "CommunityRating_",
        "CriticRating_",
        "Production_",
        "Now_",
    )

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
        if c.startswith("Score_"):
            continue
        if c.startswith(derived_prefixes):
            continue
        if c.startswith(PROVIDER_PREFIXES):
            continue
        keep.append(c)
    return df[keep].copy()


def _is_yes(v: object) -> bool:
    return str(v or "").strip().upper() in {"YES", "Y", "TRUE", "1"}


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


def _build_provider_clients(
    *,
    sources: set[str],
    credentials: dict[str, object],
    cache_dir: Path,
) -> dict[str, object]:
    """
    Instantiate provider clients from a source set, credentials, and a cache directory.
    """
    clients: dict[str, object] = {}

    if "rawg" in sources:
        api_key = str((credentials.get("rawg", {}) or {}).get("api_key", "") or "").strip()
        if api_key:
            clients["rawg"] = RAWGClient(
                api_key=api_key,
                cache_path=cache_dir / "rawg_cache.json",
                min_interval_s=RAWG.min_interval_s,
            )

    if "igdb" in sources:
        client_id = str((credentials.get("igdb", {}) or {}).get("client_id", "") or "").strip()
        secret = str((credentials.get("igdb", {}) or {}).get("client_secret", "") or "").strip()
        if client_id and secret:
            clients["igdb"] = IGDBClient(
                client_id=client_id,
                client_secret=secret,
                cache_path=cache_dir / "igdb_cache.json",
                min_interval_s=IGDB.min_interval_s,
            )

    if "steam" in sources:
        clients["steam"] = SteamClient(
            cache_path=cache_dir / "steam_cache.json",
            min_interval_s=STEAM.storesearch_min_interval_s,
        )

    if "steamspy" in sources:
        clients["steamspy"] = SteamSpyClient(
            cache_path=cache_dir / "steamspy_cache.json",
            min_interval_s=STEAMSPY.min_interval_s,
        )

    if "wikidata" in sources:
        clients["wikidata"] = WikidataClient(
            cache_path=cache_dir / "wikidata_cache.json",
            min_interval_s=WIKIDATA.min_interval_s,
        )

    if "hltb" in sources:
        clients["hltb"] = HLTBClient(cache_path=cache_dir / "hltb_cache.json")

    return clients


def _auto_unpin_likely_wrong_provider_ids(df: pd.DataFrame) -> tuple[pd.DataFrame, int, list[int]]:
    """
    If a provider was auto-pinned and diagnostics indicate it's likely wrong, clear the ID.

    This keeps the catalog safer: wrong pins are worse than missing pins because they silently
    propagate into enrichment.
    """
    out = df.copy()
    changed = 0
    changed_idx: list[int] = []

    rules: list[tuple[str, str, list[str]]] = [
        (
            "steam",
            "Steam_AppID",
            [
                "Steam_MatchedName",
                "Steam_MatchScore",
                "Steam_MatchedYear",
                "Steam_RejectedReason",
                "Steam_StoreType",
            ],
        ),
        ("rawg", "RAWG_ID", ["RAWG_MatchedName", "RAWG_MatchScore", "RAWG_MatchedYear"]),
        ("igdb", "IGDB_ID", ["IGDB_MatchedName", "IGDB_MatchScore", "IGDB_MatchedYear"]),
        (
            "hltb",
            "HLTB_ID",
            ["HLTB_MatchedName", "HLTB_MatchScore", "HLTB_MatchedYear", "HLTB_MatchedPlatforms"],
        ),
    ]

    for idx, row in out.iterrows():
        rowid = str(row.get("RowId", "") or "").strip()
        if not rowid:
            continue
        tags = str(row.get("ReviewTags", "") or "").strip()
        if not tags:
            continue
        for prov, id_col, diag_cols in rules:
            id_val = str(row.get(id_col, "") or "").strip()
            if not id_val or id_val == IDENTITY_NOT_FOUND:
                continue
            # Only auto-unpin when we have a strict-majority consensus AND this provider is
            # explicitly tagged as the outlier. This prevents unpinning in cases where providers
            # generally disagree and only a year/platform heuristic fired.
            if f"likely_wrong:{prov}" not in tags:
                continue
            if "provider_consensus:" not in tags:
                continue
            if f"provider_outlier:{prov}" not in tags:
                continue

            out.at[idx, id_col] = ""
            for c in diag_cols:
                if c in out.columns:
                    out.at[idx, c] = ""
            new_tags = tags
            if f"autounpinned:{prov}" not in new_tags:
                new_tags = (new_tags + f", autounpinned:{prov}").strip(", ").strip()
            out.at[idx, "ReviewTags"] = new_tags
            out.at[idx, "MatchConfidence"] = "LOW"
            logging.warning(
                f"[IMPORT] Auto-unpinned likely-wrong {id_col} for '{row.get('Name','')}' "
                f"(RowId={rowid})"
            )
            changed += 1
            changed_idx.append(idx)

    return out, changed, changed_idx


def load_or_merge_dataframe(input_csv: Path, output_csv: Path) -> pd.DataFrame:
    """
    Load dataframe from input CSV, merging in existing data from output CSV if it exists.

    This ensures we always process all games from the input, while preserving
    already-processed data from previous runs.
    """
    df = read_csv(input_csv)
    if "RowId" not in df.columns:
        raise SystemExit(f"Missing RowId in {input_csv}; run `import` first.")

    # If output_csv exists, merge its data to preserve already-processed games
    if output_csv.exists():
        df_output = read_csv(output_csv)
        if "RowId" not in df_output.columns:
            raise SystemExit(
                f"Missing RowId in {output_csv}; delete it and re-run, or regenerate outputs."
            )
        df = df.merge(df_output, on="RowId", how="left", suffixes=("", "_existing"))
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

    total_steam_rows = int((df_steam.get("Name", "").astype(str).str.strip() != "").sum())

    q: Queue[tuple[int, str, str] | None] = Queue()

    def steam_producer() -> None:
        processed = 0
        seen = 0
        started = time.monotonic()
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
                logging.debug(f"[STEAM] Processing: {name}")
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

            if seen % CLI.progress_every_n == 0:
                elapsed = time.monotonic() - started
                logging.info(f"[STEAM] Progress {seen}/{total_steam_rows} rows ({elapsed:.1f}s)")

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
        started = time.monotonic()
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
            if processed % 10 == 0:
                base_cols = [c for c in ("RowId", "Name") if c in df_steamspy.columns]
                steamspy_cols = base_cols + [
                    c for c in df_steamspy.columns if c.startswith("SteamSpy_")
                ]
                if "Score_SteamSpy_100" in df_steamspy.columns:
                    steamspy_cols.append("Score_SteamSpy_100")
                write_csv(df_steamspy[steamspy_cols], steamspy_output_csv)
            if processed % CLI.progress_every_n == 0:
                elapsed = time.monotonic() - started
                logging.info(f"[STEAMSPY] Progress {processed} rows ({elapsed:.1f}s)")

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
    seen = 0
    started = time.monotonic()
    total_rows = int((df.get("Name", "").astype(str).str.strip() != "").sum())

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
        seen += 1

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
                    logging.debug(f"[IGDB] Processing: {name}")
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
            score_cols = [
                c
                for c in ("Score_IGDB_100", "Score_IGDBCritic_100")
                if c in df.columns and c not in igdb_cols
            ]
            igdb_cols.extend(score_cols)
            write_csv(df[igdb_cols], output_csv)

        if seen % CLI.progress_every_n == 0:
            elapsed = time.monotonic() - started
            logging.info(f"[IGDB] Progress {seen}/{total_rows} rows ({elapsed:.1f}s)")

        if len(pending_by_id) >= CLI.igdb_flush_batch_size:
            _flush_pending()
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            igdb_cols = base_cols + [c for c in df.columns if c.startswith("IGDB_")]
            score_cols = [
                c
                for c in ("Score_IGDB_100", "Score_IGDBCritic_100")
                if c in df.columns and c not in igdb_cols
            ]
            igdb_cols.extend(score_cols)
            write_csv(df[igdb_cols], output_csv)

    _flush_pending()

    # Save only Name + IGDB columns
    base_cols = [c for c in ("RowId", "Name") if c in df.columns]
    igdb_cols = base_cols + [c for c in df.columns if c.startswith("IGDB_")]
    score_cols = [
        c
        for c in ("Score_IGDB_100", "Score_IGDBCritic_100")
        if c in df.columns and c not in igdb_cols
    ]
    igdb_cols.extend(score_cols)
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
    seen = 0
    started = time.monotonic()
    total_rows = int((df.get("Name", "").astype(str).str.strip() != "").sum())
    for idx, row in df.iterrows():
        name = row.get("Name", "").strip()
        if not name:
            continue
        seen += 1

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
            logging.debug(f"[RAWG] Processing: {name}")
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

        if seen % CLI.progress_every_n == 0:
            elapsed = time.monotonic() - started
            logging.info(f"[RAWG] Progress {seen}/{total_rows} rows ({elapsed:.1f}s)")

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
    seen = 0
    started = time.monotonic()
    total_rows = int((df.get("Name", "").astype(str).str.strip() != "").sum())

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
        seen += 1

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
            # Save only Name + Steam columns
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            steam_cols = base_cols + [c for c in df.columns if c.startswith("Steam_")]
            write_csv(df[steam_cols], output_csv)

        if seen % CLI.progress_every_n == 0:
            elapsed = time.monotonic() - started
            logging.info(f"[STEAM] Progress {seen}/{total_rows} rows ({elapsed:.1f}s)")

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
    seen = 0
    started = time.monotonic()
    total_rows = int((df.get("Steam_AppID", "").astype(str).str.strip() != "").sum())
    for idx, row in df.iterrows():
        appid = row.get("Steam_AppID", "").strip()
        name = row.get("Name", "").strip()

        if not appid:
            continue
        seen += 1

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
            # Save only Name + SteamSpy columns
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            steamspy_cols = base_cols + [c for c in df.columns if c.startswith("SteamSpy_")]
            if "Score_SteamSpy_100" in df.columns:
                steamspy_cols.append("Score_SteamSpy_100")
            write_csv(df[steamspy_cols], output_csv)
        if seen % CLI.progress_every_n == 0:
            elapsed = time.monotonic() - started
            logging.info(f"[STEAMSPY] Progress {seen}/{total_rows} rows ({elapsed:.1f}s)")

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
    seen = 0
    started = time.monotonic()
    total_rows = int((df.get("Name", "").astype(str).str.strip() != "").sum())
    for idx, row in df.iterrows():
        name = row.get("Name", "").strip()
        if not name:
            continue
        seen += 1

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
            # Save only Name + HLTB columns
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            hltb_cols = base_cols + [c for c in df.columns if c.startswith("HLTB_")]
            if "Score_HLTB_100" in df.columns:
                hltb_cols.append("Score_HLTB_100")
            write_csv(df[hltb_cols], output_csv)
        if seen % CLI.progress_every_n == 0:
            elapsed = time.monotonic() - started
            logging.info(f"[HLTB] Progress {seen}/{total_rows} rows ({elapsed:.1f}s)")

    # Save only Name + HLTB columns
    base_cols = [c for c in ("RowId", "Name") if c in df.columns]
    hltb_cols = base_cols + [c for c in df.columns if c.startswith("HLTB_")]
    if "Score_HLTB_100" in df.columns:
        hltb_cols.append("Score_HLTB_100")
    write_csv(df[hltb_cols], output_csv)
    logging.info(f"[HLTB] Cache stats: {client.format_cache_stats()}")
    logging.info(f"✔ HLTB completed: {output_csv}")


def process_wikidata(
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    required_cols: list[str],
    identity_overrides: dict[str, dict[str, str]] | None = None,
) -> None:
    """Process games with Wikidata data."""
    client = WikidataClient(cache_path=cache_path, min_interval_s=WIKIDATA.min_interval_s)
    pageviews_client = WikipediaPageviewsClient(
        cache_path=cache_path.parent / "wiki_pageviews_cache.json",
        min_interval_s=0.15,
    )
    summary_client = WikipediaSummaryClient(
        cache_path=cache_path.parent / "wiki_summary_cache.json",
        min_interval_s=0.15,
    )
    # Optional: use cached provider titles as fallback search queries for Wikidata. This helps
    # when Wikidata uses a different canonical punctuation (e.g. ":"), or when the personal
    # title is ambiguous.
    cache_dir = cache_path.parent
    steam_cache = load_json_cache(cache_dir / "steam_cache.json")
    rawg_cache = load_json_cache(cache_dir / "rawg_cache.json")
    igdb_cache = load_json_cache(cache_dir / "igdb_cache.json")
    steam_by_id = steam_cache.get("by_id") if isinstance(steam_cache, dict) else {}
    rawg_by_id = rawg_cache.get("by_id") if isinstance(rawg_cache, dict) else {}
    igdb_by_id = igdb_cache.get("by_id") if isinstance(igdb_cache, dict) else {}
    df = load_or_merge_dataframe(input_csv, output_csv)

    processed = 0
    seen = 0
    started = time.monotonic()
    total_rows = int((df.get("Name", "").astype(str).str.strip() != "").sum())
    pending_by_id: dict[str, list[int]] = {}

    def _flush_pending() -> None:
        nonlocal processed
        if not pending_by_id:
            return
        qids = list(pending_by_id.keys())
        t0 = time.monotonic()
        logging.info(
            f"[WIKIDATA] Fetching details for {len(qids)} QIDs "
            f"(processed_rows={processed}/{len(df)})..."
        )
        by_id = client.get_by_ids(qids)
        t1 = time.monotonic()
        logging.info(
            f"[WIKIDATA] Fetched details for {len(qids)} QIDs in {(t1 - t0):.1f}s "
            f"(hits={len(by_id)})"
        )
        for qid, indices in list(pending_by_id.items()):
            data = by_id.get(str(qid))
            if not data:
                continue
            enwiki_title = str(data.get("Wikidata_EnwikiTitle", "") or "").strip()
            pageviews = None
            launch = None
            if enwiki_title:
                t2 = time.monotonic()
                pageviews = pageviews_client.get_pageviews_summary_enwiki(enwiki_title)
                release_date = str(data.get("Wikidata_ReleaseDate", "") or "").strip()
                launch = pageviews_client.get_pageviews_launch_summary_enwiki(
                    enwiki_title=enwiki_title,
                    release_date=release_date,
                )
                summary = summary_client.get_summary(enwiki_title)
                t3 = time.monotonic()
                if (t3 - t2) >= 2.0:
                    logging.info(
                        f"[WIKIPEDIA] Fetched pageviews+summary for '{enwiki_title}' "
                        f"in {(t3 - t2):.1f}s"
                    )
            else:
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

    def _year_hint(row: pd.Series) -> int | None:
        yh = str(row.get("YearHint", "") or "").strip()
        if yh.isdigit() and len(yh) == 4:
            try:
                return int(yh)
            except ValueError:
                return None
        return None

    def _derived_year_hint(row: pd.Series) -> int | None:
        """
        Prefer explicit YearHint, else derive from pinned provider IDs (if any).

        This improves Wikidata matching on second runs after other providers have pinned IDs,
        without risking incorrect matches on the first run.
        """
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
                        return int(datetime.fromtimestamp(ts).year)
                    except Exception:
                        pass

        appid = str(row.get("Steam_AppID", "") or "").strip()
        if appid and isinstance(steam_by_id, dict):
            obj = steam_by_id.get(appid)
            if isinstance(obj, dict):
                date_s = str((obj.get("release_date") or {}).get("date") or "").strip()
                m = re.search(r"\b(19\\d{2}|20\\d{2})\\b", date_s)
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
        seen: set[str] = set()
        for t in titles:
            key = t.casefold()
            if key in seen:
                continue
            seen.add(key)
            out.append(t)
        return out

    for idx, row in df.iterrows():
        seen += 1
        name = str(row.get("Name", "") or "").strip()
        if not name:
            continue

        rowid = str(row.get("RowId", "") or "").strip()
        override_qid = ""
        if identity_overrides and rowid:
            override_qid = str(
                identity_overrides.get(rowid, {}).get("Wikidata_QID", "") or ""
            ).strip()

        if override_qid == IDENTITY_NOT_FOUND:
            clear_prefixed_columns(df, int(idx), "Wikidata_")
            continue

        if is_row_processed(df, idx, required_cols):
            if override_qid and str(df.at[idx, "Wikidata_QID"] or "").strip() != override_qid:
                pass
            else:
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
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            cols = base_cols + [c for c in df.columns if c.startswith("Wikidata_")]
            write_csv(df[cols], output_csv)
            elapsed = time.monotonic() - started
            logging.info(f"[WIKIDATA] Progress {seen}/{total_rows} rows ({elapsed:.1f}s)")

        if len(pending_by_id) >= WIKIDATA.get_by_ids_batch_size:
            _flush_pending()
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            cols = base_cols + [c for c in df.columns if c.startswith("Wikidata_")]
            write_csv(df[cols], output_csv)

    _flush_pending()

    base_cols = [c for c in ("RowId", "Name") if c in df.columns]
    cols = base_cols + [c for c in df.columns if c.startswith("Wikidata_")]
    write_csv(df[cols], output_csv)
    logging.info(f"[WIKIDATA] Cache stats: {client.format_cache_stats()}")
    logging.info(f"[WIKIPEDIA] Cache stats: {pageviews_client.format_cache_stats()}")
    logging.info(f"[WIKIPEDIA] Summary cache stats: {summary_client.format_cache_stats()}")
    logging.info(f"✔ Wikidata completed: {output_csv}")


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
        allowed={"igdb", "rawg", "steam", "hltb", "wikidata"},
        aliases={"core": ["igdb", "rawg", "steam"]},
    )
    diag_clients = _build_provider_clients(
        sources=set(sources),
        credentials=credentials,
        cache_dir=cache_dir,
    )

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

    if "rawg" in sources and "rawg" in diag_clients:
        client = diag_clients["rawg"]
        active_total = sum(1 for _, r in df.iterrows() if not _is_yes(r.get("Disabled", "")))
        processed = 0
        started = time.monotonic()
        logging.info(f"[RAWG] Matching IDs ({active_total} rows)...")
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
                obj = client.get_by_id(rawg_id)  # type: ignore[attr-defined]
            else:
                obj = client.search(name, year_hint=_year_hint(row))  # type: ignore[attr-defined]
                if obj and obj.get("id") is not None:
                    df.at[idx, "RAWG_ID"] = str(obj.get("id") or "").strip()
            if include_diagnostics and obj and isinstance(obj, dict):
                matched = str(obj.get("name") or "").strip()
                released = str(obj.get("released") or "").strip()
                df.at[idx, "RAWG_MatchedName"] = matched
                df.at[idx, "RAWG_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""
                df.at[idx, "RAWG_MatchedYear"] = released[:4] if len(released) >= 4 else ""
            processed += 1
            if processed % 100 == 0:
                elapsed = time.monotonic() - started
                logging.info(f"[RAWG] Progress {processed}/{active_total} ({elapsed:.1f}s)")

    if "igdb" in sources and "igdb" in diag_clients:
        client = diag_clients["igdb"]
        active_total = sum(1 for _, r in df.iterrows() if not _is_yes(r.get("Disabled", "")))
        processed = 0
        started = time.monotonic()
        logging.info(f"[IGDB] Matching IDs ({active_total} rows)...")
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
                obj = client.get_by_id(igdb_id)  # type: ignore[attr-defined]
            else:
                obj = client.search(name, year_hint=_year_hint(row))  # type: ignore[attr-defined]
                if obj and str(obj.get("IGDB_ID", "") or "").strip():
                    df.at[idx, "IGDB_ID"] = str(obj.get("IGDB_ID") or "").strip()
            if include_diagnostics and obj and isinstance(obj, dict):
                matched = str(obj.get("IGDB_Name") or "").strip()
                df.at[idx, "IGDB_MatchedName"] = matched
                df.at[idx, "IGDB_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""
                df.at[idx, "IGDB_MatchedYear"] = str(obj.get("IGDB_Year") or "").strip()
            processed += 1
            if processed % 100 == 0:
                elapsed = time.monotonic() - started
                logging.info(f"[IGDB] Progress {processed}/{active_total} ({elapsed:.1f}s)")

    if "steam" in sources and "steam" in diag_clients:
        client = diag_clients["steam"]
        active_total = sum(1 for _, r in df.iterrows() if not _is_yes(r.get("Disabled", "")))
        processed = 0
        started = time.monotonic()
        logging.info(f"[STEAM] Matching IDs ({active_total} rows)...")
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
                    df.at[idx, "Steam_RejectedReason"] = ""
                continue
            matched = ""
            matched_year = ""
            rejected_reason = ""
            store_type = ""
            if steam_id:
                if not include_diagnostics:
                    continue
                try:
                    details = client.get_app_details(int(steam_id))
                except ValueError:
                    details = None
                details_type = str((details or {}).get("type") or "").strip().lower()
                store_type = details_type
                if not details or (details_type and details_type != "game"):
                    logging.warning(
                        f"[STEAM] Ignoring pinned Steam_AppID for '{name}': appid={steam_id} "
                        f"type={details_type or 'unknown'}"
                    )
                    df.at[idx, "Steam_AppID"] = ""
                    steam_id = ""
                    if include_diagnostics:
                        matched = str((details or {}).get("name") or "").strip()
                        matched_year = ""
                        rejected_reason = f"non_game:{details_type or 'unknown'}"
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
                    store_type = details_type
                    if not details or (details_type and details_type != "game"):
                        logging.warning(
                            f"[STEAM] Ignoring inferred Steam AppID for '{name}': "
                            f"appid={inferred_ids[0]} type={details_type or 'unknown'}"
                        )
                        if include_diagnostics:
                            matched = str((details or {}).get("name") or "").strip()
                            release = (details or {}).get("release_date", {}) or {}
                            m = re.search(
                                r"\b(19\d{2}|20\d{2})\b", str(release.get("date", "") or "")
                            )
                            matched_year = m.group(1) if m else ""
                            rejected_reason = f"non_game:{details_type or 'unknown'}"
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
                            df.at[idx, "Steam_RejectedReason"] = ""
                            df.at[idx, "Steam_StoreType"] = str(details_type or "")
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
                        store_type = details_type
                        if not details or (details_type and details_type != "game"):
                            logging.warning(
                                f"[STEAM] Ignoring Steam search result for '{name}': "
                                f"appid={appid_str} type={details_type or 'unknown'}"
                            )
                            df.at[idx, "Steam_AppID"] = ""
                            matched = ""
                            matched_year = ""
                            rejected_reason = f"non_game:{details_type or 'unknown'}"
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
                df.at[idx, "Steam_RejectedReason"] = rejected_reason
                df.at[idx, "Steam_StoreType"] = store_type
            processed += 1
            if processed % 100 == 0:
                elapsed = time.monotonic() - started
                logging.info(f"[STEAM] Progress {processed}/{active_total} ({elapsed:.1f}s)")

    if "wikidata" in sources and "wikidata" in diag_clients:
        client = diag_clients["wikidata"]
        prefetched: dict[str, dict[str, str]] = {}
        if include_diagnostics and "Wikidata_QID" in df.columns:
            qids = sorted(
                {
                    str(x).strip()
                    for x in df["Wikidata_QID"].fillna("").tolist()
                    if str(x).strip() and str(x).strip() != IDENTITY_NOT_FOUND
                }
            )
            if qids:
                # Warm the wikidata cache in bulk (and fetch linked labels in batches) instead of
                # calling get_by_id() per row. This keeps warm-cache imports fast.
                prefetched = client.get_by_ids(qids)
        active_total = sum(1 for _, r in df.iterrows() if not _is_yes(r.get("Disabled", "")))
        processed = 0
        started = time.monotonic()
        logging.info(f"[WIKIDATA] Matching IDs ({active_total} rows)...")
        for idx, row in df.iterrows():
            if _is_yes(row.get("Disabled", "")):
                continue
            name = str(row.get("Name", "") or "").strip()
            if not name:
                continue
            qid = str(row.get("Wikidata_QID", "") or "").strip()
            if qid == IDENTITY_NOT_FOUND:
                if include_diagnostics:
                    df.at[idx, "Wikidata_MatchedLabel"] = ""
                    df.at[idx, "Wikidata_MatchScore"] = ""
                    df.at[idx, "Wikidata_MatchedYear"] = ""
                continue
            if qid and not include_diagnostics:
                continue

            data = None
            if not qid:
                steam_appid = str(row.get("Steam_AppID", "") or "").strip()
                igdb_id = str(row.get("IGDB_ID", "") or "").strip()
                data = client.resolve_by_hints(steam_appid=steam_appid, igdb_id=igdb_id)
            if data is None:
                if qid:
                    data = prefetched.get(qid) or client.get_by_id(qid)
                else:
                    data = client.search(name, year_hint=_year_hint(row))
            if data and str(data.get("Wikidata_QID", "") or "").strip():
                prefetched[str(data.get("Wikidata_QID") or "").strip()] = data
            if data and str(data.get("Wikidata_QID", "") or "").strip() and not qid:
                df.at[idx, "Wikidata_QID"] = str(data.get("Wikidata_QID") or "").strip()
            if include_diagnostics:
                matched = str((data or {}).get("Wikidata_Label") or "").strip()
                df.at[idx, "Wikidata_MatchedLabel"] = matched
                df.at[idx, "Wikidata_MatchScore"] = (
                    str(fuzzy_score(name, matched)) if matched else ""
                )
                df.at[idx, "Wikidata_MatchedYear"] = str(
                    (data or {}).get("Wikidata_ReleaseYear") or ""
                ).strip()
            processed += 1
            if processed % 100 == 0:
                elapsed = time.monotonic() - started
                logging.info(f"[WIKIDATA] Progress {processed}/{active_total} ({elapsed:.1f}s)")

    if "hltb" in sources and "hltb" in diag_clients:
        client = diag_clients["hltb"]
        active_total = sum(1 for _, r in df.iterrows() if not _is_yes(r.get("Disabled", "")))
        started = time.monotonic()
        logging.info(f"[HLTB] Matching IDs ({active_total} rows)...")
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
                elapsed = time.monotonic() - started
                logging.info(f"[HLTB] Progress {processed}/{active_total} ({elapsed:.1f}s)")
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
    if "wikidata" in diag_clients:
        logging.info(f"[WIKIDATA] Cache stats: {diag_clients['wikidata'].format_cache_stats()}")
    if "hltb" in diag_clients:
        logging.info(f"[HLTB] Cache stats: {diag_clients['hltb'].format_cache_stats()}")

    write_csv(df, out)
    logging.info(f"✔ Import matching completed: {out}")


def _command_resolve(args: argparse.Namespace) -> None:
    project_root, paths = _common_paths()
    _setup_logging_from_args(
        paths, args.log_file, args.debug, command_name="resolve", input_path=args.catalog
    )

    is_experiment = _is_under_dir(args.catalog, paths.data_experiments)
    cache_dir = args.cache or (
        (paths.data_experiments / "cache") if is_experiment else paths.data_cache
    )
    cache_dir.mkdir(parents=True, exist_ok=True)

    catalog_csv = args.catalog
    if not catalog_csv.exists():
        raise SystemExit(f"Catalog file not found: {catalog_csv}")

    out = args.out or catalog_csv
    out.parent.mkdir(parents=True, exist_ok=True)

    # Load credentials
    credentials_path = args.credentials or (project_root / "data" / "credentials.yaml")
    credentials = load_credentials(credentials_path)

    sources = _parse_sources(
        args.source,
        allowed={"igdb", "rawg", "steam", "hltb", "wikidata"},
        aliases={"core": ["igdb", "rawg", "steam"]},
    )

    apply = bool(getattr(args, "apply", False))

    df = read_csv(catalog_csv)
    if "ReviewTags" not in df.columns or "MatchConfidence" not in df.columns:
        raise SystemExit(
            f"{catalog_csv} is missing diagnostics columns; run `import --diagnostics` first."
        )

    def _parse_int_year(text: object) -> int | None:
        s = str(text or "").strip()
        if s.isdigit() and len(s) == 4:
            y = int(s)
            if 1900 <= y <= 2100:
                return y
        return None

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

    clients = _build_provider_clients(
        sources=set(sources),
        credentials=credentials,
        cache_dir=cache_dir,
    )

    df, stats = resolve_catalog_pins(
        df,
        sources=set(sources),
        clients=clients,
        retry_missing=bool(args.retry_missing),
        apply=apply,
    )

    if apply:
        write_csv(df, out)
    else:
        logging.info("Resolve dry-run: no catalog CSV was written (use --apply to persist changes).")

    logging.info(
        f"✔ Resolve completed: {out} (apply={str(apply).lower()}, attempted={stats.attempted}, "
        f"repinned={stats.repinned}, unpinned={stats.unpinned}, kept={stats.kept}, "
        f"wikidata_hint_added={stats.wikidata_hint_added})"
    )

    if "rawg" in clients:
        logging.info(f"[RAWG] Cache stats: {clients['rawg'].format_cache_stats()}")
    if "igdb" in clients:
        logging.info(f"[IGDB] Cache stats: {clients['igdb'].format_cache_stats()}")
    if "steam" in clients:
        logging.info(f"[STEAM] Cache stats: {clients['steam'].format_cache_stats()}")
    if "wikidata" in clients:
        logging.info(f"[WIKIDATA] Cache stats: {clients['wikidata'].format_cache_stats()}")
    if "hltb" in clients:
        logging.info(f"[HLTB] Cache stats: {clients['hltb'].format_cache_stats()}")

    return


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
    temp_base_csv: Path | None = None

    # Always strip provider-derived/diagnostics columns from the personal base before merging
    # provider outputs. merge_all uses a left-join and drops duplicate-suffixed columns, so if the
    # base already contains columns like Steam_StoreType (diagnostics) we'd lose the provider value.
    base_df = build_personal_base_for_enrich(read_csv(input_csv))
    temp_base_csv = output_dir / f".personal_base.{os.getpid()}.csv"
    write_csv(base_df, temp_base_csv)
    input_for_processing = temp_base_csv

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
            output_dir / "Provider_Wikidata.csv",
            output_dir / "Games_Enriched.csv",
            output_dir / "Validation_Report.csv",
        ):
            if p.exists():
                p.unlink()

    sources_to_process = _parse_sources(
        args.source,
        allowed={"igdb", "rawg", "steam", "steamspy", "hltb", "wikidata"},
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

        if source == "wikidata":
            process_wikidata(
                input_csv=input_csv,
                output_csv=output_dir / "Provider_Wikidata.csv",
                cache_path=cache_dir / "wikidata_cache.json",
                required_cols=["Wikidata_Label"],
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
        wikidata_csv=output_dir / "Provider_Wikidata.csv",
    )
    merged_df = read_csv(merge_output)
    merged_df = drop_eval_columns(merged_df)
    # Drop deprecated/always-empty columns to keep outputs tidy even when provider caches/CSVs
    # were created with older schemas.
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

    if args.validate:
        validate_out = args.validate_output or (output_dir / "Validation_Report.csv")
        merged = read_csv(merge_output)
        enabled_for_validation = {s.strip().lower() for s in sources_to_process if s.strip()}

        # If we didn't clean outputs, the merged CSV can contain provider data from previous runs
        # (e.g. core providers + new Wikidata). Include any providers that clearly have data so
        # validation isn't silently empty.
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


def _command_review(args: argparse.Namespace) -> None:
    project_root, paths = _common_paths()
    _setup_logging_from_args(
        paths, args.log_file, args.debug, command_name="review", input_path=args.catalog
    )
    catalog_csv = args.catalog or (paths.data_input / "Games_Catalog.csv")
    if not catalog_csv.exists():
        raise SystemExit(f"Catalog not found: {catalog_csv}")
    enriched_csv = args.enriched or (paths.data_output / "Games_Enriched.csv")
    out = args.out or (paths.data_output / "Review_TopRisk.csv")

    catalog_df = read_csv(catalog_csv)
    enriched_df = read_csv(enriched_csv) if enriched_csv.exists() else None
    review = build_review_csv(
        catalog_df,
        enriched_df=enriched_df,
        config=ReviewConfig(max_rows=int(args.max_rows)),
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    write_csv(review, out)
    logging.info(f"✔ Review CSV generated: {out} (rows={len(review)})")


def _command_production_tiers(args: argparse.Namespace) -> None:
    from game_catalog_builder.tools.production_tiers_updater import (
        suggest_and_update_production_tiers,
    )

    _, paths = _common_paths()
    _setup_logging_from_args(
        paths,
        args.log_file,
        args.debug,
        command_name="production-tiers",
        input_path=args.enriched,
    )

    res = suggest_and_update_production_tiers(
        enriched_csv=args.enriched,
        yaml_path=args.yaml,
        wiki_cache_path=args.wiki_cache,
        apply=args.apply,
        max_items=args.max_items,
        min_count=args.min_count,
        update_existing=args.update_existing,
        min_interval_s=args.min_interval_s,
        ensure_complete=args.ensure_complete,
        include_porting_labels=args.include_porting_labels,
        unknown_tier=args.unknown_tier,
    )
    verb = "updated" if args.apply else "suggested"
    logging.info(
        f"✔ Production tiers {verb}: +{res.added_publishers} publishers, "
        f"+{res.added_developers} developers; "
        f"unknown={res.unknown_publishers + res.unknown_developers} "
        f"(pub={res.unknown_publishers} dev={res.unknown_developers}); "
        f"unresolved={res.unresolved} conflicts={res.conflicts}"
    )


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        raise SystemExit(
            "Missing command. Use one of: import, enrich, sync, validate, production-tiers. "
            "Run `python run.py --help` for usage."
        )

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

    p_resolve = sub.add_parser(
        "resolve",
        help=(
            "Optional third pass: repin-or-unpin likely-wrong IDs and conservatively retry "
            "repinning using consensus titles/aliases"
        ),
    )
    p_resolve.add_argument(
        "--catalog",
        type=Path,
        default=Path("data/input/Games_Catalog.csv"),
        help="Catalog CSV with diagnostics (default: data/input/Games_Catalog.csv)",
    )
    p_resolve.add_argument(
        "--out",
        type=Path,
        help="Output catalog CSV (default: overwrite --catalog)",
    )
    p_resolve.add_argument(
        "--apply",
        action="store_true",
        help="Write any repin/unpin changes to disk (default: dry-run)",
    )
    p_resolve.add_argument("--cache", type=Path, help="Cache directory (default: data/cache)")
    p_resolve.add_argument(
        "--credentials", type=Path, help="Credentials YAML (default: data/credentials.yaml)"
    )
    p_resolve.add_argument(
        "--source",
        type=str,
        default="core,wikidata",
        help="Providers to use for retries (default: core,wikidata)",
    )
    p_resolve.add_argument(
        "--retry-missing",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Also attempt to fill missing provider IDs when a strict consensus exists "
            "(default: false)"
        ),
    )
    p_resolve.add_argument(
        "--log-file",
        type=Path,
        help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
    )
    p_resolve.add_argument(
        "--debug", action="store_true", help="Enable DEBUG logging (default: INFO)"
    )
    p_resolve.set_defaults(_fn=_command_resolve)

    p_enrich = sub.add_parser(
        "enrich", help="Generate provider outputs + Games_Enriched.csv from Games_Catalog.csv"
    )
    p_enrich.add_argument("input", type=Path, help="Catalog CSV (source of truth)")
    p_enrich.add_argument("--output", type=Path, help="Output directory (default: data/output)")
    p_enrich.add_argument("--cache", type=Path, help="Cache directory (default: data/cache)")
    p_enrich.add_argument(
        "--credentials", type=Path, help="Credentials YAML (default: data/credentials.yaml)"
    )
    p_enrich.add_argument("--source", type=str, default="all")
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
        "sync", help="Sync user-editable fields from Games_Enriched.csv back into Games_Catalog.csv"
    )
    p_sync.add_argument("catalog", type=Path, help="Catalog CSV to update")
    p_sync.add_argument("enriched", type=Path, help="Edited enriched CSV")
    p_sync.add_argument("--out", type=Path, help="Output catalog CSV (default: overwrite catalog)")
    p_sync.add_argument(
        "--log-file",
        type=Path,
        help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
    )
    p_sync.add_argument("--debug", action="store_true", help="Enable DEBUG logging (default: INFO)")
    p_sync.set_defaults(_fn=_command_sync_back)

    p_val = sub.add_parser(
        "validate", help="Generate validation report from an enriched CSV (read-only)"
    )
    p_val.add_argument(
        "--enriched",
        type=Path,
        help="Enriched CSV to validate (default: data/output/Games_Enriched.csv)",
    )
    p_val.add_argument("--output-dir", type=Path, help="Output directory (default: data/output)")
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
    p_val.add_argument("--debug", action="store_true", help="Enable DEBUG logging (default: INFO)")
    p_val.set_defaults(_fn=_command_validate)

    p_review = sub.add_parser(
        "review",
        help="Generate a focused review CSV from Games_Catalog.csv (+ optional Games_Enriched.csv)",
    )
    p_review.add_argument(
        "--catalog",
        type=Path,
        default=Path("data/input/Games_Catalog.csv"),
        help="Catalog CSV with diagnostics (default: data/input/Games_Catalog.csv)",
    )
    p_review.add_argument(
        "--enriched",
        type=Path,
        default=Path("data/output/Games_Enriched.csv"),
        help=(
            "Enriched CSV (optional; used to add extra context) "
            "(default: data/output/Games_Enriched.csv)"
        ),
    )
    p_review.add_argument(
        "--out",
        type=Path,
        default=Path("data/output/Review_TopRisk.csv"),
        help="Output review CSV path (default: data/output/Review_TopRisk.csv)",
    )
    p_review.add_argument(
        "--max-rows",
        type=int,
        default=200,
        help="Max rows to include (default: 200)",
    )
    p_review.add_argument(
        "--log-file",
        type=Path,
        help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
    )
    p_review.add_argument(
        "--debug", action="store_true", help="Enable DEBUG logging (default: INFO)"
    )
    p_review.set_defaults(_fn=_command_review)

    p_tiers = sub.add_parser(
        "production-tiers",
        help="Suggest/update production tiers mapping from Steam publishers/developers",
    )
    p_tiers.add_argument(
        "enriched",
        type=Path,
        help="Enriched CSV (must contain Steam_Publishers and Steam_Developers)",
    )
    p_tiers.add_argument(
        "--yaml",
        type=Path,
        default=Path("data/production_tiers.yaml"),
        help="Production tiers mapping YAML (default: data/production_tiers.yaml)",
    )
    p_tiers.add_argument(
        "--wiki-cache",
        type=Path,
        default=Path("data/cache/wiki_cache.json"),
        help="Wikipedia cache JSON (default: data/cache/wiki_cache.json)",
    )
    p_tiers.add_argument(
        "--min-count",
        type=int,
        default=1,
        help="Only consider entities appearing in >= N rows (default: 1)",
    )
    p_tiers.add_argument(
        "--max-items",
        type=int,
        default=50,
        help="Max entities to query and suggest per run (default: 50)",
    )
    p_tiers.add_argument(
        "--min-interval-s",
        type=float,
        default=0.15,
        help="Minimum delay between Wikipedia requests (default: 0.15)",
    )
    p_tiers.add_argument(
        "--apply",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Write suggestions into the YAML (default: false)",
    )
    p_tiers.add_argument(
        "--ensure-complete",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Ensure every publisher/developer in the CSV has a tier by filling remaining "
            "unmapped entities with 'Unknown' (default: true)"
        ),
    )
    p_tiers.add_argument(
        "--unknown-tier",
        type=str,
        default="Unknown",
        help="Tier value to write when unresolved (default: Unknown)",
    )
    p_tiers.add_argument(
        "--include-porting-labels",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Include porting-label entities (e.g., Aspyr/Feral) in the YAML mapping "
            "(default: true)"
        ),
    )
    p_tiers.add_argument(
        "--update-existing",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Allow updating existing YAML entries (default: false)",
    )
    p_tiers.add_argument(
        "--log-file",
        type=Path,
        help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
    )
    p_tiers.add_argument(
        "--debug", action="store_true", help="Enable DEBUG logging (default: INFO)"
    )
    p_tiers.set_defaults(_fn=_command_production_tiers)

    ns = parser.parse_args(argv)
    ns._fn(ns)
    return


if __name__ == "__main__":
    main()
