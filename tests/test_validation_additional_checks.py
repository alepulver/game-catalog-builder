from __future__ import annotations

import pandas as pd


def test_validation_flags_edition_token_disagreement() -> None:
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Alien Hominid",
                "RAWG_ID": "1",
                "RAWG_Name": "Alien Hominid",
                "IGDB_ID": "2",
                "IGDB_Name": "Alien Hominid Remastered",
                "Steam_AppID": "3",
                "Steam_Name": "Alien Hominid",
                "HLTB_Main": "1",
                "HLTB_Name": "Alien Hominid",
            }
        ]
    )

    report = generate_validation_report(df)
    row = report.iloc[0].to_dict()
    assert "IGDB:remastered" in row["EditionTokens"]
    assert "edition_disagree" in row["ValidationTags"]


def test_validation_flags_steam_dlc_like() -> None:
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Some Game",
                "Steam_AppID": "123",
                "Steam_Name": "Some Game Soundtrack",
                "Steam_Categories": "",
            }
        ]
    )

    report = generate_validation_report(df)
    row = report.iloc[0].to_dict()
    assert "steam_dlc_like" in row["ValidationTags"]
    assert "needs_review" in row["ValidationTags"]


def test_validation_flags_cyrillic_titles() -> None:
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Doom",
                "IGDB_ID": "1",
                "IGDB_Name": "Дум",
            }
        ]
    )

    report = generate_validation_report(df)
    row = report.iloc[0].to_dict()
    assert "title_non_english" in row["ValidationTags"]


def test_validation_flags_series_number_disagreement() -> None:
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Assassin's Creed",
                "RAWG_Name": "Assassin's Creed 2",
                "IGDB_Name": "Assassin's Creed 3",
            }
        ]
    )
    report = generate_validation_report(df)
    row = report.iloc[0].to_dict()
    assert "RAWG:2" in row["SeriesNumbers"]
    assert "IGDB:3" in row["SeriesNumbers"]
    assert "series_disagree" in row["ValidationTags"]


def test_validation_ignores_warhammer_40000_as_series_number() -> None:
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Warhammer 40,000: Space Marine",
                "RAWG_Name": "Warhammer 40,000: Space Marine",
                "Steam_Name": "Warhammer 40,000: Space Marine 2",
            }
        ]
    )
    report = generate_validation_report(df)
    row = report.iloc[0].to_dict()
    assert row["SeriesNumbers"] == "Steam:2"
    assert "series_disagree" not in row["ValidationTags"]


def test_validation_flags_hltb_year_and_platform_disagreement() -> None:
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Example",
                "RAWG_Name": "Example",
                "IGDB_Name": "Example",
                "Steam_Name": "Example",
                "HLTB_Name": "Example",
                "RAWG_Year": "2000",
                "IGDB_Year": "2000",
                "Steam_ReleaseYear": "2000",
                "HLTB_ReleaseYear": "2010",
                "RAWG_Platforms": "PC",
                "IGDB_Platforms": "PC",
                "Steam_Platforms": "Windows",
                "HLTB_Platforms": "PlayStation 2",
            }
        ]
    )
    report = generate_validation_report(df)
    row = report.iloc[0].to_dict()
    assert "year_disagree_hltb" in row["ValidationTags"]
    assert "platform_disagree_hltb" in row["ValidationTags"]
