"""Command-line interface for game catalog builder."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from .clients import (
    HLTBClient,
    IGDBClient,
    RAWGClient,
    SteamClient,
    SteamSpyClient,
)
from .utils import (
    ProjectPaths,
    ensure_columns,
    is_row_processed,
    load_credentials,
    merge_all,
    read_csv,
    write_csv,
    PUBLIC_DEFAULT_COLS,
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
        # Merge on Name, keeping data from output_csv where it exists
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


def process_igdb(
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    credentials: dict,
    required_cols: list[str],
) -> None:
    """Process games with IGDB data."""
    client = IGDBClient(
        client_id=credentials.get("igdb", {}).get("client_id", ""),
        client_secret=credentials.get("igdb", {}).get("client_secret", ""),
        cache_path=cache_path,
        min_interval_s=0.5,
    )

    df = load_or_merge_dataframe(input_csv, output_csv)

    processed = 0
    for idx, row in df.iterrows():
        name = row.get("Name", "").strip()
        if not name:
            continue

        if is_row_processed(df, idx, required_cols):
            continue

        logging.info(f"[IGDB] Processing: {name}")

        data = client.search(name)
        if not data:
            continue

        for k, v in data.items():
            df.at[idx, k] = v

        processed += 1
        if processed % 10 == 0:
            # Save only Name + IGDB columns
            igdb_cols = ["Name"] + [c for c in df.columns if c.startswith("IGDB_")]
            write_csv(df[igdb_cols], output_csv)

    # Save only Name + IGDB columns
    igdb_cols = ["Name"] + [c for c in df.columns if c.startswith("IGDB_")]
    write_csv(df[igdb_cols], output_csv)
    logging.info(f"✔ IGDB completed: {output_csv}")


def process_rawg(
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    credentials: dict,
    required_cols: list[str],
) -> None:
    """Process games with RAWG data."""
    client = RAWGClient(
        api_key=credentials.get("rawg", {}).get("api_key", ""),
        cache_path=cache_path,
        min_interval_s=1.0,
    )

    df = load_or_merge_dataframe(input_csv, output_csv)

    processed = 0
    for idx, row in df.iterrows():
        name = row.get("Name", "").strip()
        if not name:
            continue

        if is_row_processed(df, idx, required_cols):
            continue

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
            rawg_cols = ["Name"] + [c for c in df.columns if c.startswith("RAWG_")]
            write_csv(df[rawg_cols], output_csv)

    # Save only Name + RAWG columns
    rawg_cols = ["Name"] + [c for c in df.columns if c.startswith("RAWG_")]
    write_csv(df[rawg_cols], output_csv)
    logging.info(f"✔ RAWG completed: {output_csv}")


def process_steam(
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    required_cols: list[str],
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

        if is_row_processed(df, idx, required_cols):
            continue

        logging.info(f"[STEAM] Processing: {name}")

        search = client.search_appid(name)
        if not search:
            continue

        appid = search.get("id")
        details = client.get_app_details(appid)
        if not details:
            continue

        fields = client.extract_fields(appid, details)
        for k, v in fields.items():
            df.at[idx, k] = v

        processed += 1
        if processed % 10 == 0:
            # Save only Name + Steam columns
            steam_cols = ["Name"] + [c for c in df.columns if c.startswith("Steam_")]
            write_csv(df[steam_cols], output_csv)

    # Save only Name + Steam columns
    steam_cols = ["Name"] + [c for c in df.columns if c.startswith("Steam_")]
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
            steamspy_cols = ["Name"] + [c for c in df.columns if c.startswith("SteamSpy_")]
            write_csv(df[steamspy_cols], output_csv)

    # Save only Name + SteamSpy columns
    steamspy_cols = ["Name"] + [c for c in df.columns if c.startswith("SteamSpy_")]
    write_csv(df[steamspy_cols], output_csv)
    logging.info(f"✔ SteamSpy completed: {output_csv}")


def process_hltb(
    input_csv: Path,
    output_csv: Path,
    cache_path: Path,
    required_cols: list[str],
) -> None:
    """Process games with HowLongToBeat data."""
    client = HLTBClient(cache_path=cache_path)

    df = load_or_merge_dataframe(input_csv, output_csv)

    processed = 0
    for idx, row in df.iterrows():
        name = row.get("Name", "").strip()
        if not name:
            continue

        if is_row_processed(df, idx, required_cols):
            continue

        logging.info(f"[HLTB] Processing: {name}")

        data = client.search(name)
        if not data:
            continue

        for k, v in data.items():
            df.at[idx, k] = v

        processed += 1
        if processed % 10 == 0:
            # Save only Name + HLTB columns
            hltb_cols = ["Name"] + [c for c in df.columns if c.startswith("HLTB_")]
            write_csv(df[hltb_cols], output_csv)

    # Save only Name + HLTB columns
    hltb_cols = ["Name"] + [c for c in df.columns if c.startswith("HLTB_")]
    write_csv(df[hltb_cols], output_csv)
    logging.info(f"✔ HLTB completed: {output_csv}")


def setup_logging(log_file: Path) -> None:
    """Configure logging to both console and file."""
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_formatter = logging.Formatter('%(message)s')

    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

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
        help="Output directory for processed files (default: data/processed)",
    )
    parser.add_argument(
        "--cache",
        type=Path,
        help="Cache directory for API responses (default: data/raw)",
    )
    parser.add_argument(
        "--credentials",
        type=Path,
        help="Path to credentials.yaml file (default: credentials.yaml in project root)",
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
        help="Output file for merged results (default: data/processed/Games_Final.csv)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Log file path (default: output/enrichment.log)",
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
        log_file = project_root / "output" / "enrichment.log"

    setup_logging(log_file)
    logging.info("Starting game catalog enrichment")

    # Set up paths
    input_csv = args.input
    if not input_csv.exists():
        parser.error(f"Input file not found: {input_csv}")

    output_dir = args.output or paths.data_processed
    output_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = args.cache or paths.data_raw
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Load credentials
    if args.credentials:
        credentials_path = args.credentials
    else:
        # Default: look for credentials.yaml in project root
        credentials_path = project_root / "credentials.yaml"

    credentials = load_credentials(credentials_path)

    # Process based on source
    sources_to_process = (
        ["igdb", "rawg", "steam", "steamspy", "hltb"]
        if args.source == "all"
        else [args.source]
    )

    for source in sources_to_process:
        if source == "igdb":
            process_igdb(
                input_csv=input_csv,
                output_csv=output_dir / "Games_IGDB.csv",
                cache_path=cache_dir / "igdb_cache.json",
                credentials=credentials,
                required_cols=["IGDB_ID"],
            )
        elif source == "rawg":
            process_rawg(
                input_csv=input_csv,
                output_csv=output_dir / "Games_RAWG.csv",
                cache_path=cache_dir / "rawg_cache.json",
                credentials=credentials,
                required_cols=["RAWG_ID", "RAWG_Year", "RAWG_Genre"],
            )
        elif source == "steam":
            process_steam(
                input_csv=input_csv,
                output_csv=output_dir / "Games_Steam.csv",
                cache_path=cache_dir / "steam_cache.json",
                required_cols=["Steam_AppID"],
            )
        elif source == "steamspy":
            process_steamspy(
                input_csv=output_dir / "Games_Steam.csv",
                output_csv=output_dir / "Games_SteamSpy.csv",
                cache_path=cache_dir / "steamspy_cache.json",
                required_cols=["SteamSpy_Owners"],
            )
        elif source == "hltb":
            process_hltb(
                input_csv=input_csv,
                output_csv=output_dir / "Games_HLTB.csv",
                cache_path=cache_dir / "hltb_cache.json",
                required_cols=["HLTB_Main"],
            )

    # Merge if requested
    if args.merge or args.source == "all":
        merge_output = args.merge_output or (output_dir / "Games_Final.csv")
        merge_all(
            personal_csv=input_csv,
            rawg_csv=output_dir / "Games_RAWG.csv",
            hltb_csv=output_dir / "Games_HLTB.csv",
            steam_csv=output_dir / "Games_Steam.csv",
            steamspy_csv=output_dir / "Games_SteamSpy.csv",
            output_csv=merge_output,
            igdb_csv=output_dir / "Games_IGDB.csv",
        )
        logging.info(f"✔ Games_Final.csv generated successfully: {merge_output}")


if __name__ == "__main__":
    main()

