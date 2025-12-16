"""Command-line interface for game catalog builder."""

from __future__ import annotations

import argparse
import logging
import os
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
    PUBLIC_DEFAULT_COLS,
    ProjectPaths,
    ensure_columns,
    ensure_row_ids,
    generate_identity_map,
    generate_validation_report,
    is_row_processed,
    load_credentials,
    load_identity_overrides,
    merge_all,
    merge_identity_user_fields,
    normalize_game_name,
    read_csv,
    write_csv,
)


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

            if is_row_processed(df_steam, int(idx), ["Steam_AppID"]):
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


def main() -> None:
    """Main CLI entry point."""
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
        help="Log file path (default: data/output/enrichment.log)",
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
    parser.add_argument(
        "--identity-map",
        action="store_true",
        help="Generate an identity mapping CSV (IDs, matched names, match scores) (default: off)",
    )
    parser.add_argument(
        "--identity-map-output",
        type=Path,
        help="Output file for identity mapping (default: data/output/Games_Identity.csv)",
    )
    parser.add_argument(
        "--identity-map-input",
        type=Path,
        help="Identity map to apply as overrides (default: <output>/Games_Identity.csv if present)",
    )

    args = parser.parse_args()

    # Determine project root (parent of game_catalog_builder package)
    project_root = Path(__file__).resolve().parent.parent
    paths = ProjectPaths.from_root(project_root)
    paths.ensure()

    # Set up logging (after paths are ensured)
    if args.log_file:
        log_file = args.log_file
    else:
        log_file = paths.data_output / "enrichment.log"

    setup_logging(log_file)
    if args.debug:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        for handler in root_logger.handlers:
            handler.setLevel(logging.DEBUG)
        logging.getLogger("urllib3").setLevel(logging.DEBUG)
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

    # Load identity overrides (if present) for fixed-ID processing.
    identity_map_in = args.identity_map_input or (output_dir / "Games_Identity.csv")
    identity_overrides: dict[str, dict[str, str]] = {}
    if identity_map_in and identity_map_in.exists():
        identity_overrides = load_identity_overrides(identity_map_in)
        logging.info(
            f"✔ Using identity overrides: {identity_map_in} (rows: {len(identity_overrides)})"
        )

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
                required_cols=["IGDB_ID"],
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
                required_cols=["Steam_AppID"],
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

        validation_df = None
        if args.validate:
            validate_out = args.validate_output or (output_dir / "Validation_Report.csv")
            merged = read_csv(merge_output)
            report = generate_validation_report(merged)
            write_csv(report, validate_out)
            validation_df = report

            issues = report[
                (report.get("YearDisagree_RAWG_IGDB", "") == "YES")
                | (report.get("PlatformDisagree", "") == "YES")
                | (report.get("SteamAppIDMismatch", "") == "YES")
                | (report.get("TitleMismatch", "") == "YES")
            ]

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

            # Validation stats summary.
            def _count_yes(col: str) -> int:
                if col not in report.columns:
                    return 0
                return int(report[col].astype(str).str.strip().eq("YES").sum())

            logging.info(
                f"✔ Validation report generated: {validate_out} "
                f"(rows with issues: {len(issues)}/{len(report)})"
            )
            logging.info(
                "✔ Validation stats: "
                f"title_mismatch={_count_yes('TitleMismatch')}, "
                f"year_disagree_rawg_igdb={_count_yes('YearDisagree_RAWG_IGDB')}, "
                f"steam_year_disagree={_count_yes('SteamYearDisagree')}, "
                f"platform_disagree={_count_yes('PlatformDisagree')}, "
                f"steam_appid_mismatch={_count_yes('SteamAppIDMismatch')}"
            )
            for _, row in issues.head(20).iterrows():
                logging.warning(
                    f"[VALIDATE] {row.get('Name', '')}: "
                    f"TitleMismatch={row.get('TitleMismatch', '') or 'NO'}, "
                    f"YearDisagree_RAWG_IGDB={row.get('YearDisagree_RAWG_IGDB', '') or 'NO'}, "
                    f"PlatformDisagree={row.get('PlatformDisagree', '') or 'NO'}, "
                    f"SteamAppIDMismatch={row.get('SteamAppIDMismatch', '') or 'NO'}, "
                    f"Culprit={row.get('SuggestedCulprit', '') or ''}, "
                    f"Canonical={row.get('SuggestedCanonicalTitle', '') or ''} "
                    f"({row.get('SuggestedCanonicalSource', '') or ''})"
                )

        if args.identity_map:
            identity_out = args.identity_map_output or (output_dir / "Games_Identity.csv")
            merged = read_csv(merge_output)
            identity = generate_identity_map(merged, validation_df)
            # Preserve AddedAt across regenerations by joining on RowId when available.
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if identity_out.exists():
                prev = read_csv(identity_out)
                identity = merge_identity_user_fields(identity, prev)
                if (
                    "RowId" in identity.columns
                    and "RowId" in prev.columns
                    and "AddedAt" in prev.columns
                ):
                    prev_map = dict(zip(prev["RowId"].astype(str), prev["AddedAt"].astype(str)))
                    identity["AddedAt"] = identity["RowId"].astype(str).map(prev_map).fillna("")
            if "AddedAt" not in identity.columns:
                identity["AddedAt"] = ""
            if "RowId" in identity.columns:
                new_mask = identity["AddedAt"].astype(str).str.strip() == ""
                identity.loc[new_mask, "AddedAt"] = now
                identity["NewRow"] = new_mask.map(lambda x: "YES" if x else "")
            else:
                identity["NewRow"] = ""
            write_csv(identity, identity_out)
            needs_review = (identity.get("NeedsReview", "").astype(str).str.strip() == "YES").sum()
            new_rows = (identity.get("NewRow", "").astype(str).str.strip() == "YES").sum()
            logging.info(
                f"✔ Identity map generated: {identity_out} "
                f"(needs review: {needs_review}/{len(identity)})"
            )
            # Summary stats for identification review (keep detailed per-row output at DEBUG).
            tags_counter: dict[str, int] = {}
            for t in (
                identity.get("ReviewTags", []).tolist() if "ReviewTags" in identity.columns else []
            ):
                for part in str(t or "").split(","):
                    part = part.strip()
                    if not part:
                        continue
                    key = part.split(":", 1)[0].strip()
                    tags_counter[key] = tags_counter.get(key, 0) + 1

            def _count(tag: str) -> int:
                return int(tags_counter.get(tag, 0))

            logging.info(
                "✔ Identity stats: needs_review=%s, new_rows=%s, missing_rawg=%s, missing_igdb=%s, "
                "missing_steam=%s, missing_hltb=%s, title_mismatch=%s, year_mismatch=%s, "
                "platform_mismatch=%s, steam_appid_mismatch=%s, rawg_score_lt_100=%s, "
                "igdb_score_lt_100=%s, steam_score_lt_100=%s, hltb_score_lt_100=%s",
                needs_review,
                new_rows,
                _count("missing_rawg"),
                _count("missing_igdb"),
                _count("missing_steam"),
                _count("missing_hltb"),
                _count("title_mismatch"),
                _count("year_mismatch"),
                _count("platform_mismatch"),
                _count("steam_appid_mismatch"),
                _count("rawg_score"),
                _count("igdb_score"),
                _count("steam_score"),
                _count("hltb_score"),
            )
            for _, row in identity[identity["NeedsReview"] == "YES"].iterrows():
                rawg_name = row.get("RAWG_MatchedName", "")
                rawg_score = row.get("RAWG_MatchScore", "") or "-"
                igdb_name = row.get("IGDB_MatchedName", "")
                igdb_score = row.get("IGDB_MatchScore", "") or "-"
                steam_name = row.get("Steam_MatchedName", "")
                steam_score = row.get("Steam_MatchScore", "") or "-"
                hltb_name = row.get("HLTB_MatchedName", "")
                hltb_score = row.get("HLTB_MatchScore", "") or "-"

                rawg_part = f"RAWG={rawg_name}({rawg_score})"
                igdb_part = f"IGDB={igdb_name}({igdb_score})"
                steam_part = f"Steam={steam_name}({steam_score})"
                hltb_part = f"HLTB={hltb_name}({hltb_score})"
                msg = " ".join(
                    [
                        f"[IDENTITY] {row.get('RowId', '')}: '{row.get('OriginalName', '')}'",
                        f"NewRow={row.get('NewRow', '') or 'NO'}",
                        f"AddedAt={row.get('AddedAt', '') or ''}",
                        f"Tags={row.get('ReviewTags', '') or ''}",
                        rawg_part,
                        igdb_part,
                        steam_part,
                        hltb_part,
                    ]
                )
                logging.debug(msg)


if __name__ == "__main__":
    main()
