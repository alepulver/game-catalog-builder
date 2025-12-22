from __future__ import annotations

import pandas as pd


def test_auto_unpin_then_fill_eval_tags_drops_stale_provider_score_tags() -> None:
    from game_catalog_builder.cli import _auto_unpin_likely_wrong_provider_ids, fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Diablo",
                "Steam_AppID": "111",
                "Steam_MatchedName": "Diablo IV",
                "Steam_MatchScore": "70",
                "Steam_MatchedYear": "2023",
                "Steam_RejectedReason": "",
                "Steam_StoreType": "game",
                "ReviewTags": "provider_consensus:igdb+rawg+hltb, provider_outlier:steam, "
                "year_outlier:steam, likely_wrong:steam, steam_score:70",
                "MatchConfidence": "LOW",
            }
        ]
    )

    out, changed, changed_idx = _auto_unpin_likely_wrong_provider_ids(df)
    assert changed == 1
    assert changed_idx == [0]
    assert out.loc[0, "Steam_AppID"] == ""
    assert out.loc[0, "Steam_MatchScore"] == ""
    assert "autounpinned:steam" in out.loc[0, "ReviewTags"]

    # Simulate the import/resolve behavior: recompute tags based on the final row state.
    recomputed = fill_eval_tags(out)
    tags = recomputed.loc[0, "ReviewTags"]
    assert "steam_score:" not in tags
    assert "autounpinned:steam" in tags

