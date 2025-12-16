from __future__ import annotations

import pandas as pd


def test_generate_identity_map_includes_scores_and_review_flag():
    from game_catalog_builder.utils.identity import generate_identity_map

    merged = pd.DataFrame(
        [
            {
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
            {"ReviewTitle": "", "TitleMismatch": ""},
            {"ReviewTitle": "YES", "TitleMismatch": "YES"},
        ]
    )

    identity = generate_identity_map(merged, validation)
    assert identity.iloc[0]["InputRowKey"].startswith("row:")
    assert identity.iloc[0]["RAWG_MatchScore"] == "100"
    assert identity.iloc[1]["NeedsReview"] == "YES"
    assert identity.iloc[1]["ReviewTitle"] == "YES"

