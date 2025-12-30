from __future__ import annotations

import pandas as pd


def test_validation_adds_dev_pub_disagree_tags_when_sets_disjoint():
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Example",
                "RAWG_ID": "1",
                "RAWG_Name": "Example",
                "IGDB_ID": "2",
                "IGDB_Name": "Example",
                "Steam_AppID": "3",
                "Steam_Name": "Example",
                "Steam_Developers": ["Valve"],
                "RAWG_Developers": ["id Software"],
                "IGDB_Developers": ["id Software"],
                "Steam_Publishers": ["Valve"],
                "RAWG_Publishers": ["Bethesda"],
                "IGDB_Publishers": ["Bethesda"],
            }
        ]
    )
    report = generate_validation_report(df, enabled_providers={"steam", "rawg", "igdb"})
    tags = report.loc[0, "ValidationTags"]
    assert "developer_disagree" in tags
    assert "publisher_disagree" in tags
    assert "developer_outlier:steam" in tags
    assert "publisher_outlier:steam" in tags


def test_validation_does_not_flag_dev_disagree_when_provider_lists_bridge() -> None:
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Example",
                "RAWG_ID": "1",
                "RAWG_Name": "Example",
                "IGDB_ID": "2",
                "IGDB_Name": "Example",
                "Steam_AppID": "3",
                "Steam_Name": "Example",
                "Steam_Developers": ["Studio A", "Studio B"],
                "RAWG_Developers": ["Studio A"],
                "IGDB_Developers": ["Studio B"],
            }
        ]
    )
    report = generate_validation_report(df, enabled_providers={"steam", "rawg", "igdb"})
    tags = report.loc[0, "ValidationTags"]
    assert "developer_disagree" not in tags
    assert "developer_outlier:steam" not in tags


def test_validation_does_not_flag_dev_disagree_when_no_majority_component() -> None:
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Example",
                "RAWG_ID": "1",
                "RAWG_Name": "Example",
                "IGDB_ID": "2",
                "IGDB_Name": "Example",
                "Steam_AppID": "3",
                "Steam_Name": "Example",
                "Steam_Developers": ["Studio A"],
                "RAWG_Developers": ["Studio B"],
                "IGDB_Developers": ["Studio C"],
            }
        ]
    )
    report = generate_validation_report(df, enabled_providers={"steam", "rawg", "igdb"})
    tags = report.loc[0, "ValidationTags"]
    assert "developer_disagree" not in tags
