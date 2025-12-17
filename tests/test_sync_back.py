from __future__ import annotations

from pathlib import Path

import pandas as pd


def test_sync_back_updates_user_fields_and_keeps_provider_fields_out(tmp_path: Path) -> None:
    from game_catalog_builder.cli import _sync_back_catalog

    catalog_csv = tmp_path / "Games_Catalog.csv"
    enriched_csv = tmp_path / "Games_Enriched.csv"
    out_csv = tmp_path / "Games_Catalog.updated.csv"

    catalog = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Doom",
                "MyRating": "5",
                "RAWG_ID": "123",
                "IGDB_ID": "456",
                "Steam_AppID": "620",
                "HLTB_Query": "",
            },
            {"RowId": "rid:2", "Name": "Old Name", "MyRating": "1", "Disabled": ""},
        ]
    )
    catalog.to_csv(catalog_csv, index=False)

    enriched = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "DOOM (2016)",
                "MyRating": "4",
                "Notes": "Great",
                "RAWG_ID": "123",
                "RAWG_Name": "DOOM",
                "Steam_Name": "DOOM",
            },
            {
                "RowId": "rid:3",
                "Name": "New Game",
                "MyRating": "2",
                "RAWG_Name": "New Game (RAWG)",
            },
        ]
    )
    enriched.to_csv(enriched_csv, index=False)

    _sync_back_catalog(catalog_csv=catalog_csv, enriched_csv=enriched_csv, output_csv=out_csv)
    out = pd.read_csv(out_csv).fillna("")

    # Updated from enriched.
    r1 = out[out["RowId"] == "rid:1"].iloc[0].to_dict()
    assert r1["Name"] == "DOOM (2016)"
    assert str(r1["MyRating"]) == "4"
    assert r1["Notes"] == "Great"

    # Provider-derived columns should not be copied back or introduced into the catalog.
    assert "RAWG_Name" not in out.columns
    # Evaluation/diagnostic columns should not be present in the synced catalog.
    assert "ReviewTags" not in out.columns
    assert "NeedsReview" not in out.columns

    # Missing in enriched => disabled.
    r2 = out[out["RowId"] == "rid:2"].iloc[0].to_dict()
    assert r2["Disabled"] == "YES"

    # New row in enriched should be appended.
    assert (out["RowId"] == "rid:3").any()
