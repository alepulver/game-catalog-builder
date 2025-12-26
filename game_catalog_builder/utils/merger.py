from __future__ import annotations

from pathlib import Path

import pandas as pd

from .signals import apply_phase1_signals
from .utilities import read_csv, write_csv


def merge_left(base: pd.DataFrame, other: pd.DataFrame) -> pd.DataFrame:
    """
    Left-join provider data onto the personal base using RowId.
    """
    if "RowId" not in base.columns or "RowId" not in other.columns:
        raise ValueError(
            "Missing RowId in base/provider CSV; run `import` and provider steps again."
        )
    return base.merge(other, on="RowId", how="left", suffixes=("", "_dup"))


def drop_duplicate_suffixes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove *_dup columns created by redundant merges.
    """
    dup_cols = [c for c in df.columns if c.endswith("_dup")]
    return df.drop(columns=dup_cols)


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Logical order: Personal → Signals → Scores → RAWG → IGDB → Steam → SteamSpy → HLTB
    → Wikidata
    """
    personal_cols = [
        "RowId",
        "Name",
        "Status",
        "Rating",
        "Play again",
        "Platform",
        "Added to List On",
        "Notes",
    ]

    signal_cols = [
        "Reach_SteamSpyOwners_Low",
        "Reach_SteamSpyOwners_High",
        "Reach_SteamSpyOwners_Mid",
        "Reach_Composite",
        "Reach_SteamReviews",
        "Reach_RAWGRatingsCount",
        "Reach_IGDBRatingCount",
        "Reach_IGDBAggregatedRatingCount",
        "Launch_Interest_100",
        "CommunityRating_Composite_100",
        "CriticRating_Composite_100",
        "Developers_ConsensusProviders",
        "Developers_Consensus",
        "Developers_ConsensusProviderCount",
        "Publishers_ConsensusProviders",
        "Publishers_Consensus",
        "Publishers_ConsensusProviderCount",
        "ContentType",
        "ContentType_ConsensusProviders",
        "ContentType_SourceSignals",
        "ContentType_Conflict",
        "Production_Tier",
        "Production_TierReason",
        "Now_SteamSpyPlaytimeAvg2Weeks",
        "Now_SteamSpyPlaytimeMedian2Weeks",
        "Now_Composite",
    ]

    rawg_cols = [c for c in df.columns if c.startswith("RAWG_")]
    igdb_cols = [c for c in df.columns if c.startswith("IGDB_")]
    steam_cols = [c for c in df.columns if c.startswith("Steam_")]
    steamspy_cols = [c for c in df.columns if c.startswith("SteamSpy_")]
    hltb_cols = [c for c in df.columns if c.startswith("HLTB_")]
    wikidata_cols = [c for c in df.columns if c.startswith("Wikidata_")]
    score_cols = [c for c in df.columns if c.startswith("Score_")]

    ordered = (
        personal_cols
        + signal_cols
        + score_cols
        + rawg_cols
        + igdb_cols
        + steam_cols
        + steamspy_cols
        + hltb_cols
        + wikidata_cols
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
    wikidata_csv: Path | None = None,
):
    # Personal base (NEVER overwritten)
    df = read_csv(personal_csv)
    if "RowId" not in df.columns:
        raise ValueError("Missing RowId in personal CSV; run `import` first.")

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

    if wikidata_csv and wikidata_csv.exists():
        df = merge_left(df, read_csv(wikidata_csv))
        df = drop_duplicate_suffixes(df)

    df = apply_phase1_signals(df)

    # Final order
    df = reorder_columns(df)

    write_csv(df, output_csv)
