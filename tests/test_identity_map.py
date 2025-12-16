from __future__ import annotations

import pandas as pd


def test_generate_identity_map_includes_scores_and_review_flag():
    from game_catalog_builder.utils.identity import generate_identity_map

    merged = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Doom (2016)",
                "RAWG_ID": "1",
                "RAWG_Name": "DOOM (2016)",
                "IGDB_ID": "2",
                "IGDB_Name": "Doom",
                "Steam_AppID": "3",
                "Steam_Name": "DOOM",
                "HLTB_Name": "Doom",
            },
            {
                "RowId": "rid:2",
                "Name": "Air Conflicts",
                "RAWG_ID": "10",
                "RAWG_Name": "Air Conflicts: Vietnam",
                "IGDB_ID": "20",
                "IGDB_Name": "Air Conflicts",
                "Steam_AppID": "30",
                "Steam_Name": "Air Conflicts: Vietnam",
                "HLTB_Name": "Air Conflicts",
            },
        ]
    )

    validation = pd.DataFrame(
        [
            {"TitleMismatch": ""},
            {"TitleMismatch": "YES", "YearDisagree_RAWG_IGDB": "YES"},
        ]
    )

    identity = generate_identity_map(merged, validation)
    assert identity.iloc[0]["RowId"] == "rid:1"
    assert "InputRowKey" not in identity.columns
    assert identity.iloc[0]["RAWG_MatchScore"] == "100"
    assert identity.iloc[1]["NeedsReview"] == "YES"
    assert "title_mismatch" in identity.iloc[1]["ReviewTags"]
