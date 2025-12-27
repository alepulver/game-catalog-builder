from __future__ import annotations

from pathlib import Path

import pandas as pd


def test_normalize_catalog_adds_rowid_and_identity_columns(tmp_path: Path) -> None:
    from game_catalog_builder.pipelines.import_pipeline import normalize_catalog

    inp = tmp_path / "input.csv"
    out = tmp_path / "Games_Catalog.csv"

    pd.DataFrame([{"Name": "Doom", "MyRating": "5"}]).to_csv(inp, index=False)
    normalize_catalog(inp, out)

    df = pd.read_csv(out).fillna("")
    assert "RowId" in df.columns
    assert df.iloc[0]["RowId"]
    for c in (
        "RAWG_ID",
        "IGDB_ID",
        "Steam_AppID",
        "HLTB_Query",
        "Wikidata_QID",
        "Disabled",
    ):
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
        "Wikidata_MatchedLabel",
        "Wikidata_MatchScore",
        "ReviewTags",
        "MatchConfidence",
    ):
        assert c in df.columns


def test_normalize_catalog_can_skip_diagnostics_columns(tmp_path: Path) -> None:
    from game_catalog_builder.pipelines.import_pipeline import normalize_catalog

    inp = tmp_path / "input.csv"
    out = tmp_path / "Games_Catalog.csv"

    pd.DataFrame([{"Name": "Doom", "MyRating": "5"}]).to_csv(inp, index=False)
    normalize_catalog(inp, out, include_diagnostics=False)

    df = pd.read_csv(out).fillna("")
    assert "RowId" in df.columns
    for c in (
        "RAWG_ID",
        "IGDB_ID",
        "Steam_AppID",
        "HLTB_Query",
        "Wikidata_QID",
        "Disabled",
    ):
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
        "Wikidata_MatchedLabel",
        "Wikidata_MatchScore",
        "ReviewTags",
        "MatchConfidence",
    ):
        assert c not in df.columns
