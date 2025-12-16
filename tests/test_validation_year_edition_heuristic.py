from __future__ import annotations

import pandas as pd


def test_validation_steam_year_is_downweighted_for_editions():
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Alien Hominid",
                "RAWG_ID": "1",
                "RAWG_Name": "Alien Hominid",
                "RAWG_Year": "2004",
                "RAWG_Platforms": "Xbox",
                "IGDB_ID": "2",
                "IGDB_Name": "Alien Hominid",
                "IGDB_Year": "2004",
                "IGDB_Platforms": "Xbox",
                "Steam_AppID": "123",
                "Steam_Name": "Alien Hominid HD",
                "Steam_ReleaseYear": "2023",
                "Steam_Platforms": "Windows",
                "HLTB_Main": "1",
                "HLTB_Name": "Alien Hominid",
                "SteamSpy_Owners": "1",
            }
        ]
    )

    report = generate_validation_report(df)
    row = report.iloc[0].to_dict()
    # Steam year drift should not trigger SteamYearDisagree due to "HD" token.
    assert row["SteamEditionOrPort"] == "YES"
    assert row["SteamYearDisagree"] == ""
    # RAWG/IGDB agree; overall YearDisagree should remain empty.
    assert row["YearDisagree_RAWG_IGDB"] == ""
    assert row["YearDisagree"] == ""

