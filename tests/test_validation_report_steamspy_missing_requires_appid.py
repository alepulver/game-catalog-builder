from __future__ import annotations

import pandas as pd


def test_validation_report_does_not_mark_steamspy_missing_without_steam_appid():
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Non Steam Game",
                "Steam_AppID": "",
                "SteamSpy_Owners": "",
                "RAWG_ID": "1",
                "RAWG_Name": "Non Steam Game",
            },
            {
                "Name": "Steam Game",
                "Steam_AppID": "123",
                "SteamSpy_Owners": "",
                "RAWG_ID": "2",
                "RAWG_Name": "Steam Game",
            },
        ]
    )
    report = generate_validation_report(df, enabled_providers={"steamspy", "rawg", "steam"})
    tags0 = report.loc[0, "ValidationTags"]
    tags1 = report.loc[1, "ValidationTags"]
    assert "missing:SteamSpy" not in tags0
    assert "missing:SteamSpy" in tags1

