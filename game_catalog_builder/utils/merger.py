from __future__ import annotations

from pathlib import Path
from typing import List
import pandas as pd

from .utilities import read_csv, write_csv


def merge_left(base: pd.DataFrame, other: pd.DataFrame, on: str = "Name") -> pd.DataFrame:
    """
    Safe left-join: preserves all rows from base.
    """
    return base.merge(other, on=on, how="left", suffixes=("", "_dup"))


def drop_duplicate_suffixes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove *_dup columns created by redundant merges.
    """
    dup_cols = [c for c in df.columns if c.endswith("_dup")]
    return df.drop(columns=dup_cols)


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Logical order: Personal → RAWG → IGDB → Steam → SteamSpy → HLTB
    """
    personal_cols = [
        "Name",
        "Status",
        "Rating",
        "Play again",
        "Platform",
        "Added to List On",
        "Notes",
    ]

    rawg_cols = [c for c in df.columns if c.startswith("RAWG_")]
    igdb_cols = [c for c in df.columns if c.startswith("IGDB_")]
    steam_cols = [c for c in df.columns if c.startswith("Steam_")]
    steamspy_cols = [c for c in df.columns if c.startswith("SteamSpy_")]
    hltb_cols = [c for c in df.columns if c.startswith("HLTB_")]

    ordered = (
        personal_cols
        + rawg_cols
        + igdb_cols
        + steam_cols
        + steamspy_cols
        + hltb_cols
    )

    existing = [c for c in ordered if c in df.columns]
    remaining = [c for c in df.columns if c not in existing]

    return df[existing + remaining]


def merge_all(
    personal_csv: Path,
    rawg_csv: Path,
    hltb_csv: Path,
    steam_csv: Path,
    steamspy_csv: Path,
    output_csv: Path,
    igdb_csv: Path,
):
    # Personal base (NEVER overwritten)
    df = read_csv(personal_csv)

    # Incremental merges
    if rawg_csv.exists():
        df = merge_left(df, read_csv(rawg_csv))
        df = drop_duplicate_suffixes(df)

    if hltb_csv.exists():
        df = merge_left(df, read_csv(hltb_csv))
        df = drop_duplicate_suffixes(df)

    if steam_csv.exists():
        df = merge_left(df, read_csv(steam_csv))
        df = drop_duplicate_suffixes(df)

    if steamspy_csv.exists():
        df = merge_left(df, read_csv(steamspy_csv))
        df = drop_duplicate_suffixes(df)

    if igdb_csv.exists():
        df = merge_left(df, read_csv(igdb_csv))
        df = drop_duplicate_suffixes(df)

    # Final order
    df = reorder_columns(df)

    write_csv(df, output_csv)
