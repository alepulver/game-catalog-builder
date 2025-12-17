from __future__ import annotations

import pandas as pd


def test_build_personal_base_for_enrich_strips_provider_columns() -> None:
    from game_catalog_builder.cli import build_personal_base_for_enrich

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Doom",
                "MyRating": "5",
                "RAWG_ID": "123",
                "RAWG_Name": "DOOM",
                "IGDB_ID": "456",
                "IGDB_Name": "Doom",
                "Steam_AppID": "620",
                "Steam_Name": "DOOM",
                "SteamSpy_Owners": "1..2",
                "HLTB_Query": "",
                "HLTB_Name": "Doom",
                "RAWG_MatchScore": "100",
                "NeedsReview": "YES",
            }
        ]
    )
    out = build_personal_base_for_enrich(df)
    cols = set(out.columns)
    assert "RAWG_ID" in cols
    assert "IGDB_ID" in cols
    assert "Steam_AppID" in cols
    assert "HLTB_Query" in cols
    assert "MyRating" in cols

    # Derived provider columns should be stripped.
    assert "RAWG_Name" not in cols
    assert "IGDB_Name" not in cols
    assert "Steam_Name" not in cols
    assert "SteamSpy_Owners" not in cols
    assert "HLTB_Name" not in cols

    # Eval columns should be stripped.
    assert "RAWG_MatchScore" not in cols
    assert "NeedsReview" not in cols

