from __future__ import annotations

from pathlib import Path

import pandas as pd


def test_normalize_catalog_adds_rowid_and_identity_columns(tmp_path: Path) -> None:
    from game_catalog_builder.cli import _normalize_catalog

    inp = tmp_path / "input.csv"
    out = tmp_path / "Games_Catalog.csv"

    pd.DataFrame([{"Name": "Doom", "MyRating": "5"}]).to_csv(inp, index=False)
    _normalize_catalog(inp, out)

    df = pd.read_csv(out).fillna("")
    assert "RowId" in df.columns
    assert df.iloc[0]["RowId"]
    for c in ("RAWG_ID", "IGDB_ID", "Steam_AppID", "HLTB_Query", "Disabled"):
        assert c in df.columns
    for c in (
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
    ):
        assert c in df.columns
