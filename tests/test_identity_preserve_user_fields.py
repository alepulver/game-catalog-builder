from __future__ import annotations

import pandas as pd


def test_merge_identity_user_fields_preserves_non_empty_ids():
    from game_catalog_builder.utils.identity import merge_identity_user_fields

    new = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "RAWG_ID": "10",
                "IGDB_ID": "20",
                "Steam_AppID": "30",
                "HLTB_Query": "",
            },
            {"RowId": "rid:2", "RAWG_ID": "11", "IGDB_ID": "", "Steam_AppID": "", "HLTB_Query": ""},
        ]
    )
    prev = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "RAWG_ID": "999",
                "IGDB_ID": "",
                "Steam_AppID": "333",
                "HLTB_Query": "doom",
            },
            {
                "RowId": "rid:2",
                "RAWG_ID": "",
                "IGDB_ID": "222",
                "Steam_AppID": "",
                "HLTB_Query": "",
            },
        ]
    )

    merged = merge_identity_user_fields(new, prev)
    r1 = merged[merged["RowId"] == "rid:1"].iloc[0].to_dict()
    assert r1["RAWG_ID"] == "999"  # prev wins (non-empty)
    assert r1["IGDB_ID"] == "20"  # new kept (prev empty)
    assert r1["Steam_AppID"] == "333"  # prev wins
    assert r1["HLTB_Query"] == "doom"  # prev wins

    r2 = merged[merged["RowId"] == "rid:2"].iloc[0].to_dict()
    assert r2["RAWG_ID"] == "11"  # new kept (prev empty)
    assert r2["IGDB_ID"] == "222"  # prev wins
