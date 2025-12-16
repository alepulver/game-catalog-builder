from __future__ import annotations

import pandas as pd


def test_validation_suggests_consensus_canonical_title():
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Air Conflicts",
                "RAWG_ID": "1",
                "RAWG_Name": "Air Conflicts: Vietnam",
                "RAWG_Year": "2013",
                "RAWG_Platforms": "PC",
                "IGDB_ID": "2",
                "IGDB_Name": "Air Conflicts",
                "IGDB_Year": "2006",
                "IGDB_Platforms": "PC",
                "Steam_AppID": "3",
                "Steam_Name": "Air Conflicts: Vietnam",
                "Steam_ReleaseYear": "2013",
                "Steam_Platforms": "Windows",
                "HLTB_Main": "1",
                "HLTB_Name": "Air Conflicts",
                "SteamSpy_Owners": "1",
            }
        ]
    )

    report = generate_validation_report(df)
    row = report.iloc[0].to_dict()
    assert row["SuggestedCanonicalTitle"] == "Air Conflicts: Vietnam"
    assert row["SuggestedCanonicalSource"] in ("Steam", "RAWG")
    assert row["SuggestedRenamePersonalName"] == "YES"
    assert row["ReviewTitle"] == "YES"
    assert row["ReviewTitleReason"] != ""
    assert row["SuggestionReason"].startswith("provider consensus:")
