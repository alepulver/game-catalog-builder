from __future__ import annotations

import pandas as pd


def test_fill_eval_tags_marks_missing_and_not_found() -> None:
    from game_catalog_builder.pipelines.diagnostics.import_diagnostics import fill_eval_tags
    from game_catalog_builder.utils import IDENTITY_NOT_FOUND

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "A",
                "RAWG_ID": "",
                "IGDB_ID": IDENTITY_NOT_FOUND,
                "Steam_AppID": "123",
                "Steam_MatchedName": "A",
                "HLTB_Query": "",
                "HLTB_MatchedName": "",
                "RAWG_MatchScore": "",
                "IGDB_MatchScore": "",
                "Steam_MatchScore": "100",
                "HLTB_MatchScore": "",
            }
        ]
    )
    out = fill_eval_tags(df)
    assert out.iloc[0]["MatchConfidence"] == "MEDIUM"
    tags = out.iloc[0]["ReviewTags"]
    assert "missing_rawg" in tags
    assert "igdb_not_found" in tags
    assert "missing_hltb" in tags


def test_fill_eval_tags_ignores_disabled_rows() -> None:
    from game_catalog_builder.pipelines.diagnostics.import_diagnostics import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "A",
                "Disabled": "YES",
                "RAWG_ID": "",
                "IGDB_ID": "",
                "Steam_AppID": "",
                "HLTB_Query": "",
                "HLTB_MatchedName": "",
                "RAWG_MatchScore": "",
                "IGDB_MatchScore": "",
                "Steam_MatchScore": "",
                "HLTB_MatchScore": "",
            }
        ]
    )
    out = fill_eval_tags(df)
    assert out.iloc[0]["MatchConfidence"] == ""
    assert "disabled" in out.iloc[0]["ReviewTags"]
