from __future__ import annotations

import pandas as pd


def test_generate_validation_report_flags_mismatches():
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Doom (2016)",
                "RAWG_ID": "1",
                "RAWG_Name": "DOOM",
                "RAWG_Year": "2016",
                "RAWG_Platforms": "PC",
                "IGDB_ID": "2",
                "IGDB_Name": "Doom",
                "IGDB_Year": "2016",
                "IGDB_Platforms": "PC (Microsoft Windows)",
                "IGDB_SteamAppID": "379720",
                "Steam_AppID": "379720",
                "Steam_Name": "DOOM",
                "Steam_ReleaseYear": "2016",
                "Steam_Platforms": "Windows",
                "HLTB_Main": "10",
                "HLTB_Name": "DOOM",
                "SteamSpy_Owners": "1 .. 2",
            },
            {
                "Name": "Doom (2016)",
                "RAWG_ID": "1",
                "RAWG_Name": "Doom",
                "RAWG_Year": "2016",
                "RAWG_Platforms": "PC",
                "IGDB_ID": "999",
                "IGDB_Name": "Doom 3",
                "IGDB_Year": "2004",
                "IGDB_Platforms": "Xbox",
                "IGDB_SteamAppID": "999",
                "Steam_AppID": "379720",
                "Steam_Name": "DOOM",
                "Steam_ReleaseYear": "2016",
                "Steam_Platforms": "Windows",
                "HLTB_Main": "",
                "HLTB_Name": "",
                "SteamSpy_Owners": "",
            },
        ]
    )

    report = generate_validation_report(df)
    assert list(report.columns)[0] == "Name"

    ok = report.iloc[0].to_dict()
    assert ok["TitleMismatch"] == ""
    assert ok["YearDisagree"] == ""
    assert ok["YearDisagree_RAWG_IGDB"] == ""
    assert ok["SteamYearDisagree"] == ""
    assert ok["PlatformDisagree"] == ""
    assert ok["SteamAppIDMismatch"] == ""
    assert ok["SuggestedCulprit"] == ""

    bad = report.iloc[1].to_dict()
    assert bad["TitleMismatch"] == "YES"
    assert bad["YearDisagree"] == "YES"
    assert bad["YearDisagree_RAWG_IGDB"] == "YES"
    assert bad["PlatformDisagree"] == "YES"
    assert bad["SteamAppIDMismatch"] == "YES"
    assert bad["SuggestedCulprit"] in ("IGDB", "RAWG", "Steam", "HLTB", "RAWG/IGDB")
    assert bad["SuggestedCanonicalTitle"] == "DOOM"
    assert bad["SuggestedCanonicalSource"] in ("Steam", "RAWG", "IGDB", "HLTB")
    assert bad["SuggestedRenamePersonalName"] == "YES"
