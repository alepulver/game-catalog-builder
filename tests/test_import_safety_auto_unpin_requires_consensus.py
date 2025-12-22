from __future__ import annotations

import pandas as pd


def test_auto_unpin_requires_consensus_and_outlier_tag():
    from game_catalog_builder.cli import _auto_unpin_likely_wrong_provider_ids

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Diablo",
                "Steam_AppID": "111",
                "Steam_MatchedName": "Diablo IV",
                "Steam_MatchScore": "70",
                "Steam_MatchedYear": "2023",
                "Steam_StoreType": "game",
                # has likely_wrong but no consensus/outlier tags -> should NOT unpin
                "ReviewTags": "likely_wrong:steam, year_outlier:steam",
                "MatchConfidence": "LOW",
            },
            {
                "RowId": "rid:2",
                "Name": "Assassin's Creed",
                "Steam_AppID": "222",
                "Steam_MatchedName": "Assassin's Creed Unity",
                "Steam_MatchScore": "83",
                "Steam_MatchedYear": "2014",
                "Steam_StoreType": "game",
                # has consensus + outlier + likely_wrong -> should unpin
                "ReviewTags": "provider_consensus:igdb+rawg+hltb, provider_outlier:steam, "
                "year_outlier:steam, likely_wrong:steam",
                "MatchConfidence": "LOW",
            },
        ]
    )

    out, changed, changed_idx = _auto_unpin_likely_wrong_provider_ids(df)
    assert changed == 1
    assert changed_idx == [1]
    assert out.loc[0, "Steam_AppID"] == "111"
    assert out.loc[1, "Steam_AppID"] == ""
    assert out.loc[1, "Steam_StoreType"] == ""
    assert "autounpinned:steam" in out.loc[1, "ReviewTags"]
