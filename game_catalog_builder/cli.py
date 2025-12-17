"""Command-line interface for game catalog builder."""

from __future__ import annotations

import argparse
import logging
import os
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
from .utils import (
    IDENTITY_NOT_FOUND,
    PUBLIC_DEFAULT_COLS,
    ProjectPaths,
    ensure_columns,
    ensure_row_ids,
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


def clear_prefixed_columns(df: pd.DataFrame, idx: int, prefix: str) -> None:
    for c in [col for col in df.columns if col.startswith(prefix)]:
        df.at[idx, c] = ""


EVAL_COLUMNS = [
    "RAWG_MatchedName",
    "RAWG_MatchScore",
    "IGDB_MatchedName",
    "IGDB_MatchScore",
    "Steam_MatchedName",
    "Steam_MatchScore",
    "HLTB_MatchedName",
    "HLTB_MatchScore",
    "ReviewTags",
    "NeedsReview",
]


def drop_eval_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in EVAL_COLUMNS if c in df.columns]
    return df.drop(columns=cols) if cols else df


PROVIDER_PREFIXES = ("RAWG_", "IGDB_", "Steam_", "SteamSpy_", "HLTB_")
PINNED_ID_COLS = {"RAWG_ID", "IGDB_ID", "Steam_AppID", "HLTB_Query"}


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


def fill_eval_tags(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = ensure_columns(out, {"ReviewTags": "", "NeedsReview": ""})

    tags_list: list[str] = []
    needs_list: list[str] = []

    for _, row in out.iterrows():
        tags: list[str] = []
        disabled = _is_yes(row.get("Disabled", ""))
        if disabled:
            tags.append("disabled")

        missing = False
        rawg_id = str(row.get("RAWG_ID", "") or "").strip()
        if rawg_id == IDENTITY_NOT_FOUND:
            tags.append("rawg_not_found")
        elif not rawg_id:
            tags.append("missing_rawg")
            missing = True

        igdb_id = str(row.get("IGDB_ID", "") or "").strip()
        if igdb_id == IDENTITY_NOT_FOUND:
            tags.append("igdb_not_found")
        elif not igdb_id:
            tags.append("missing_igdb")
            missing = True

        steam_id = str(row.get("Steam_AppID", "") or "").strip()
        if steam_id == IDENTITY_NOT_FOUND:
            tags.append("steam_not_found")
        elif not steam_id:
            tags.append("missing_steam")
            missing = True

        hltb_query = str(row.get("HLTB_Query", "") or "").strip()
        hltb_name = str(row.get("HLTB_MatchedName", "") or "").strip()
        if hltb_query == IDENTITY_NOT_FOUND:
            tags.append("hltb_not_found")
        elif not hltb_name:
            tags.append("missing_hltb")
            missing = True

        low_score = False
        for score_col, tag_prefix in (
            ("RAWG_MatchScore", "rawg_score"),
            ("IGDB_MatchScore", "igdb_score"),
            ("Steam_MatchScore", "steam_score"),
            ("HLTB_MatchScore", "hltb_score"),
        ):
            s = str(row.get(score_col, "") or "").strip()
            if s.isdigit() and int(s) < 100:
                tags.append(f"{tag_prefix}:{s}")
                low_score = True

        # Not-found sentinel is considered resolved; missing/low scores need review.
        needs_review = (not disabled) and (missing or low_score)
        tags_list.append(", ".join(tags))
        needs_list.append("YES" if needs_review else "")

    out["ReviewTags"] = pd.Series(tags_list)
    out["NeedsReview"] = pd.Series(needs_list)
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
    steam_client = SteamClient(cache_path=steam_cache_path, min_interval_s=0.5)
    steamspy_client = SteamSpyClient(cache_path=steamspy_cache_path, min_interval_s=0.5)

    df_steam = load_or_merge_dataframe(input_csv, steam_output_csv)
    df_steamspy = read_csv(input_csv)
    df_steamspy = ensure_columns(df_steamspy, PUBLIC_DEFAULT_COLS)

    q: Queue[tuple[int, str, str] | None] = Queue()

    def steam_producer() -> None:
        processed = 0
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

            details = steam_client.get_app_details(int(appid))
            if not details:
                continue

            fields = steam_client.extract_fields(int(appid), details)
            for k, v in fields.items():
                df_steam.at[idx, k] = v

            processed += 1
            if processed % 10 == 0:
                base_cols = [c for c in ("RowId", "Name") if c in df_steam.columns]
                steam_cols = base_cols + [c for c in df_steam.columns if c.startswith("Steam_")]
                write_csv(df_steam[steam_cols], steam_output_csv)

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
                write_csv(df_steamspy[steamspy_cols], steamspy_output_csv)

        base_cols = [c for c in ("RowId", "Name") if c in df_steamspy.columns]
        steamspy_cols = base_cols + [c for c in df_steamspy.columns if c.startswith("SteamSpy_")]
        write_csv(df_steamspy[steamspy_cols], steamspy_output_csv)

    with ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(steam_producer)
        f2 = executor.submit(steamspy_consumer)
        f1.result()
        f2.result()


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
        min_interval_s=0.3,
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
            data = client.get_by_id(override_id)
            if not data:
                logging.warning(f"[IGDB] Override id not found: {name} (IGDB_ID {override_id})")
                continue
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
            write_csv(df[igdb_cols], output_csv)

    # Save only Name + IGDB columns
    base_cols = [c for c in ("RowId", "Name") if c in df.columns]
    igdb_cols = base_cols + [c for c in df.columns if c.startswith("IGDB_")]
    write_csv(df[igdb_cols], output_csv)
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
        min_interval_s=0.5,
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
            write_csv(df[rawg_cols], output_csv)

    # Save only Name + RAWG columns
    base_cols = [c for c in ("RowId", "Name") if c in df.columns]
    rawg_cols = base_cols + [c for c in df.columns if c.startswith("RAWG_")]
    write_csv(df[rawg_cols], output_csv)
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
        min_interval_s=0.5,
    )

    df = load_or_merge_dataframe(input_csv, output_csv)

    processed = 0
    for idx, row in df.iterrows():
        name = row.get("Name", "").strip()
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

        if override_appid:
            appid = int(override_appid)
        else:
            logging.info(f"[STEAM] Processing: {name}")
            search = client.search_appid(name)
            if not search:
                continue
            appid = search.get("id")
            if not appid:
                continue

        details = client.get_app_details(appid)
        if not details:
            continue

        fields = client.extract_fields(appid, details)
        for k, v in fields.items():
            df.at[idx, k] = v

        processed += 1
        if processed % 10 == 0:
            # Save only Name + Steam columns
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            steam_cols = base_cols + [c for c in df.columns if c.startswith("Steam_")]
            write_csv(df[steam_cols], output_csv)

    # Save only Name + Steam columns
    base_cols = [c for c in ("RowId", "Name") if c in df.columns]
    steam_cols = base_cols + [c for c in df.columns if c.startswith("Steam_")]
    write_csv(df[steam_cols], output_csv)
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
        min_interval_s=0.5,
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
            write_csv(df[steamspy_cols], output_csv)

    # Save only Name + SteamSpy columns
    base_cols = [c for c in ("RowId", "Name") if c in df.columns]
    steamspy_cols = base_cols + [c for c in df.columns if c.startswith("SteamSpy_")]
    write_csv(df[steamspy_cols], output_csv)
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
        if identity_overrides and rowid:
            query = str(identity_overrides.get(rowid, {}).get("HLTB_Query", "") or "").strip()
        if query == IDENTITY_NOT_FOUND:
            clear_prefixed_columns(df, int(idx), "HLTB_")
            continue
        query = query or name

        if is_row_processed(df, idx, required_cols):
            prev_name = str(df.at[idx, "HLTB_Name"] or "").strip()
            if prev_name and normalize_game_name(prev_name) == normalize_game_name(query):
                continue

        logging.info(f"[HLTB] Processing: {query}")
        data = client.search(query)
        if not data:
            continue

        for k, v in data.items():
            df.at[idx, k] = v

        processed += 1
        if processed % 10 == 0:
            # Save only Name + HLTB columns
            base_cols = [c for c in ("RowId", "Name") if c in df.columns]
            hltb_cols = base_cols + [c for c in df.columns if c.startswith("HLTB_")]
            write_csv(df[hltb_cols], output_csv)

    # Save only Name + HLTB columns
    base_cols = [c for c in ("RowId", "Name") if c in df.columns]
    hltb_cols = base_cols + [c for c in df.columns if c.startswith("HLTB_")]
    write_csv(df[hltb_cols], output_csv)
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


def _default_log_file(paths: ProjectPaths, *, command_name: str) -> Path:
    logs_dir = paths.data_logs
    logs_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    stamp = now.strftime("%Y%m%d-%H%M%S") + f".{now.microsecond // 1000:03d}"
    base = f"{command_name}-{stamp}.log"
    candidate = logs_dir / base
    if not candidate.exists():
        return candidate

    for i in range(2, 1000):
        p = logs_dir / f"{command_name}-{stamp}-{i}.log"
        if not p.exists():
            return p
    return logs_dir / f"{command_name}-{stamp}-{os.getpid()}.log"


def _setup_logging_from_args(
    paths: ProjectPaths, log_file: Path | None, debug: bool, *, command_name: str
) -> None:
    setup_logging(log_file or _default_log_file(paths, command_name=command_name))
    if debug:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        for handler in root_logger.handlers:
            handler.setLevel(logging.DEBUG)
        logging.getLogger("urllib3").setLevel(logging.DEBUG)

    argv = " ".join(shlex.quote(a) for a in sys.argv)
    logging.info(f"Invocation: {argv}")


def _normalize_catalog(input_csv: Path, output_csv: Path) -> Path:
    df = read_csv(input_csv)
    if "Name" not in df.columns:
        raise SystemExit(f"Missing required column 'Name' in {input_csv}")

    if "RowId" in df.columns:
        rowid = df["RowId"].astype(str).str.strip()
        if rowid.duplicated().any():
            raise SystemExit(f"Duplicate RowId values in {input_csv}; fix them before importing.")

    df = ensure_columns(
        df,
        {
            "RowId": "",
            "Name": "",
            "Disabled": "",
            "RAWG_ID": "",
            "IGDB_ID": "",
            "Steam_AppID": "",
            "HLTB_Query": "",
            **{c: "" for c in EVAL_COLUMNS},
        },
    )
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
    provider_id_cols = {"RAWG_ID", "IGDB_ID", "Steam_AppID", "HLTB_Query"}
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
        help="Log file path (default: data/logs/<command>-<timestamp>.log)",
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
    _setup_logging_from_args(paths, args.log_file, args.debug, command_name="legacy")
    logging.info("Starting game catalog enrichment")

    # Set up paths
    input_csv = args.input
    if not input_csv.exists():
        parser.error(f"Input file not found: {input_csv}")
    before = read_csv(input_csv)
    with_ids, created = ensure_row_ids(before)
    # If we had to create RowIds, avoid overwriting the user's original file: write a new input
    # next to the identity map (under the output dir) and use it from now on.
    if created > 0 or "RowId" not in before.columns:
        safe_input = (
            args.output or paths.data_output
        ) / f"{input_csv.stem}_with_rowid{input_csv.suffix}"
        write_csv(with_ids, safe_input)
        logging.info(f"✔ RowId initialized: wrote new input CSV: {safe_input} (new ids: {created})")
        logging.info(f"ℹ Use this input file going forward: {safe_input}")
        input_csv = safe_input

    output_dir = args.output or paths.data_output
    output_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = args.cache or paths.data_cache
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
            top = sorted(tags_counter.items(), key=lambda kv: kv[1], reverse=True)[:10]
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
    _setup_logging_from_args(paths, args.log_file, args.debug, command_name="import")
    cache_dir = args.cache or paths.data_cache
    cache_dir.mkdir(parents=True, exist_ok=True)

    out = args.out or (paths.data_input / "Games_Catalog.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    _normalize_catalog(args.input, out)

    # Provider matching to populate pinned IDs and diagnostics.
    credentials_path = args.credentials or (project_root / "data" / "credentials.yaml")
    credentials = load_credentials(credentials_path)

    df = read_csv(out)
    sources = (
        ["igdb", "rawg", "steam", "hltb"] if args.source == "all" else [args.source]
    )

    if "rawg" in sources:
        api_key = credentials.get("rawg", {}).get("api_key", "")
        if api_key:
            client = RAWGClient(
                api_key=api_key,
                cache_path=cache_dir / "rawg_cache.json",
                min_interval_s=0.5,
            )
            for idx, row in df.iterrows():
                if _is_yes(row.get("Disabled", "")):
                    continue
                name = str(row.get("Name", "") or "").strip()
                if not name:
                    continue
                rawg_id = str(row.get("RAWG_ID", "") or "").strip()
                if rawg_id == IDENTITY_NOT_FOUND:
                    df.at[idx, "RAWG_MatchedName"] = ""
                    df.at[idx, "RAWG_MatchScore"] = ""
                    continue
                if rawg_id:
                    obj = client.get_by_id(rawg_id)
                else:
                    obj = client.search(name)
                    if obj and obj.get("id") is not None:
                        df.at[idx, "RAWG_ID"] = str(obj.get("id") or "").strip()
                if obj and isinstance(obj, dict):
                    matched = str(obj.get("name") or "").strip()
                    df.at[idx, "RAWG_MatchedName"] = matched
                    df.at[idx, "RAWG_MatchScore"] = (
                        str(fuzzy_score(name, matched)) if matched else ""
                    )

    if "igdb" in sources:
        client_id = credentials.get("igdb", {}).get("client_id", "")
        secret = credentials.get("igdb", {}).get("client_secret", "")
        if client_id and secret:
            client = IGDBClient(
                client_id=client_id,
                client_secret=secret,
                cache_path=cache_dir / "igdb_cache.json",
                min_interval_s=0.3,
            )
            for idx, row in df.iterrows():
                if _is_yes(row.get("Disabled", "")):
                    continue
                name = str(row.get("Name", "") or "").strip()
                if not name:
                    continue
                igdb_id = str(row.get("IGDB_ID", "") or "").strip()
                if igdb_id == IDENTITY_NOT_FOUND:
                    df.at[idx, "IGDB_MatchedName"] = ""
                    df.at[idx, "IGDB_MatchScore"] = ""
                    continue
                if igdb_id:
                    obj = client.get_by_id(igdb_id)
                else:
                    obj = client.search(name)
                    if obj and str(obj.get("IGDB_ID", "") or "").strip():
                        df.at[idx, "IGDB_ID"] = str(obj.get("IGDB_ID") or "").strip()
                if obj and isinstance(obj, dict):
                    matched = str(obj.get("IGDB_Name") or "").strip()
                    df.at[idx, "IGDB_MatchedName"] = matched
                    df.at[idx, "IGDB_MatchScore"] = (
                        str(fuzzy_score(name, matched)) if matched else ""
                    )

    if "steam" in sources:
        client = SteamClient(cache_path=cache_dir / "steam_cache.json", min_interval_s=0.5)
        for idx, row in df.iterrows():
            if _is_yes(row.get("Disabled", "")):
                continue
            name = str(row.get("Name", "") or "").strip()
            if not name:
                continue
            steam_id = str(row.get("Steam_AppID", "") or "").strip()
            if steam_id == IDENTITY_NOT_FOUND:
                df.at[idx, "Steam_MatchedName"] = ""
                df.at[idx, "Steam_MatchScore"] = ""
                continue
            if steam_id:
                try:
                    details = client.get_app_details(int(steam_id))
                except ValueError:
                    details = None
                matched = str((details or {}).get("name") or "").strip()
            else:
                search = client.search_appid(name)
                matched = str((search or {}).get("name") or "").strip()
                if search and search.get("id") is not None:
                    df.at[idx, "Steam_AppID"] = str(search.get("id") or "").strip()
            df.at[idx, "Steam_MatchedName"] = matched
            df.at[idx, "Steam_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""

    if "hltb" in sources:
        client = HLTBClient(cache_path=cache_dir / "hltb_cache.json")
        for idx, row in df.iterrows():
            if _is_yes(row.get("Disabled", "")):
                continue
            name = str(row.get("Name", "") or "").strip()
            if not name:
                continue
            query = str(row.get("HLTB_Query", "") or "").strip()
            if query == IDENTITY_NOT_FOUND:
                df.at[idx, "HLTB_MatchedName"] = ""
                df.at[idx, "HLTB_MatchScore"] = ""
                continue
            q = query or name
            data = client.search(q)
            matched = str((data or {}).get("HLTB_Name") or "").strip()
            df.at[idx, "HLTB_MatchedName"] = matched
            df.at[idx, "HLTB_MatchScore"] = str(fuzzy_score(name, matched)) if matched else ""

    df = fill_eval_tags(df)
    write_csv(df, out)
    logging.info(f"✔ Import matching completed: {out}")


def _command_enrich(args: argparse.Namespace) -> None:
    project_root, paths = _common_paths()
    _setup_logging_from_args(paths, args.log_file, args.debug, command_name="enrich")
    logging.info("Starting game catalog enrichment")

    input_csv = args.input
    if not input_csv.exists():
        raise SystemExit(f"Input file not found: {input_csv}")

    output_dir = args.output or paths.data_output
    output_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = args.cache or paths.data_cache
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
        report = generate_validation_report(merged)
        write_csv(report, validate_out)
        logging.info(f"✔ Validation report generated: {validate_out}")

    if temp_base_csv and temp_base_csv.exists():
        temp_base_csv.unlink()


def _command_sync_back(args: argparse.Namespace) -> None:
    _, paths = _common_paths()
    _setup_logging_from_args(paths, args.log_file, args.debug, command_name="sync")
    out = args.out or args.catalog
    _sync_back_catalog(catalog_csv=args.catalog, enriched_csv=args.enriched, output_csv=out)


def _command_validate(args: argparse.Namespace) -> None:
    _, paths = _common_paths()
    _setup_logging_from_args(paths, args.log_file, args.debug, command_name="validate")
    output_dir = args.output_dir or paths.data_output
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
            help="Log file path (default: data/logs/<command>-<timestamp>.log)",
        )
        p_import.add_argument("--cache", type=Path, help="Cache directory (default: data/cache)")
        p_import.add_argument(
            "--credentials", type=Path, help="Credentials YAML (default: data/credentials.yaml)"
        )
        p_import.add_argument(
            "--source",
            choices=["igdb", "rawg", "steam", "hltb", "all"],
            default="all",
            help="Which providers to match for IDs (default: all)",
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
            choices=["igdb", "rawg", "steam", "steamspy", "hltb", "all"],
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
            help="Log file path (default: data/logs/<command>-<timestamp>.log)",
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
            help="Log file path (default: data/logs/<command>-<timestamp>.log)",
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
            help="Log file path (default: data/logs/<command>-<timestamp>.log)",
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
