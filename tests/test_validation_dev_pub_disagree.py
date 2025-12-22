from __future__ import annotations

import json

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
                "Steam_Developers": json.dumps(["Valve"], ensure_ascii=False),
                "RAWG_Developers": json.dumps(["id Software"], ensure_ascii=False),
                "IGDB_Developers": json.dumps(["id Software"], ensure_ascii=False),
                "Steam_Publishers": json.dumps(["Valve"], ensure_ascii=False),
                "RAWG_Publishers": json.dumps(["Bethesda"], ensure_ascii=False),
                "IGDB_Publishers": json.dumps(["Bethesda"], ensure_ascii=False),
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
                "Steam_Developers": json.dumps(["Studio A", "Studio B"], ensure_ascii=False),
                "RAWG_Developers": json.dumps(["Studio A"], ensure_ascii=False),
                "IGDB_Developers": json.dumps(["Studio B"], ensure_ascii=False),
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
                "Steam_Developers": json.dumps(["Studio A"], ensure_ascii=False),
                "RAWG_Developers": json.dumps(["Studio B"], ensure_ascii=False),
                "IGDB_Developers": json.dumps(["Studio C"], ensure_ascii=False),
            }
        ]
    )
    report = generate_validation_report(df, enabled_providers={"steam", "rawg", "igdb"})
    tags = report.loc[0, "ValidationTags"]
    assert "developer_disagree" not in tags
