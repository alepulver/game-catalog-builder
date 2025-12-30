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
                "RAWG_Platforms": ["PC"],
                "IGDB_ID": "2",
                "IGDB_Name": "Doom",
                "IGDB_Year": "2016",
                "IGDB_Platforms": ["PC (Microsoft Windows)"],
                "IGDB_SteamAppID": "379720",
                "Steam_AppID": "379720",
                "Steam_Name": "DOOM",
                "Steam_ReleaseYear": "2016",
                "Steam_Platforms": ["Windows"],
                "HLTB_Main": "10",
                "HLTB_Name": "DOOM",
                "SteamSpy_Owners": "1 .. 2",
            },
            {
                "Name": "Doom (2016)",
                "RAWG_ID": "1",
                "RAWG_Name": "Doom",
                "RAWG_Year": "2016",
                "RAWG_Platforms": ["PC"],
                "IGDB_ID": "999",
                "IGDB_Name": "Doom 3",
                "IGDB_Year": "2004",
                "IGDB_Platforms": ["Xbox"],
                "IGDB_SteamAppID": "999",
                "Steam_AppID": "379720",
                "Steam_Name": "DOOM",
                "Steam_ReleaseYear": "2016",
                "Steam_Platforms": ["Windows"],
                "HLTB_Main": "",
                "HLTB_Name": "",
                "SteamSpy_Owners": "",
            },
        ]
    )

    report = generate_validation_report(df)
    assert list(report.columns)[0] == "Name"
    assert "ValidationTags" in report.columns

    ok = report.iloc[0].to_dict()
    # Even when providers agree, we may still ask for review if the personal title differs from a
    # strong canonical suggestion (e.g. extra year tokens in the personal title).
    ok_tags = ok["ValidationTags"]
    # Consensus tags may be present, but the "ok" row should not contain mismatch flags.
    assert "title_mismatch" not in ok_tags
    assert "year_disagree" not in ok_tags
    assert "platform_disagree" not in ok_tags
    assert "steam_appid_mismatch" not in ok_tags
    assert ok["SuggestedCulprit"] == ""

    bad = report.iloc[1].to_dict()
    assert "title_mismatch" in bad["ValidationTags"]
    assert "year_disagree_rawg_igdb" in bad["ValidationTags"]
    assert "platform_disagree" in bad["ValidationTags"]
    assert "steam_appid_mismatch" in bad["ValidationTags"]
    assert "missing:HLTB" in bad["ValidationTags"]
    assert "missing:SteamSpy" in bad["ValidationTags"]
    assert bad["SuggestedCulprit"] in ("IGDB", "RAWG", "Steam", "HLTB", "RAWG/IGDB")
    assert bad["SuggestedCanonicalTitle"] == "DOOM"
    assert bad["SuggestedCanonicalSource"] in ("Steam", "RAWG", "IGDB", "HLTB")
    assert bad["ReviewTitle"] == "YES"
    assert bad["ReviewTitleReason"] != ""
