from __future__ import annotations

import pandas as pd


def test_auto_unpin_then_fill_eval_tags_drops_stale_provider_score_tags() -> None:
    from game_catalog_builder.metrics.registry import MetricsRegistry
    from game_catalog_builder.pipelines.diagnostics.import_diagnostics import fill_eval_tags
    from game_catalog_builder.pipelines.diagnostics.resolve import auto_unpin_likely_wrong_provider_ids

    registry = MetricsRegistry(
        by_key={},
        by_column={},
        diagnostics_by_key={
            "diagnostics.steam.matched_name": ("Steam_MatchedName", "string"),
            "diagnostics.steam.match_score": ("Steam_MatchScore", "int"),
            "diagnostics.steam.matched_year": ("Steam_MatchedYear", "int"),
            "diagnostics.steam.rejected_reason": ("Steam_RejectedReason", "string"),
            "diagnostics.review.tags": ("ReviewTags", "string"),
            "diagnostics.match.confidence": ("MatchConfidence", "string"),
        },
        diagnostics_by_column={
            "Steam_MatchedName": ("diagnostics.steam.matched_name", "string"),
            "Steam_MatchScore": ("diagnostics.steam.match_score", "int"),
            "Steam_MatchedYear": ("diagnostics.steam.matched_year", "int"),
            "Steam_RejectedReason": ("diagnostics.steam.rejected_reason", "string"),
            "ReviewTags": ("diagnostics.review.tags", "string"),
            "MatchConfidence": ("diagnostics.match.confidence", "string"),
        },
    )

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
                "ReviewTags": "provider_consensus:igdb+rawg+hltb, provider_outlier:steam, "
                "year_outlier:steam, likely_wrong:steam, steam_score:70",
                "MatchConfidence": "LOW",
            }
        ]
    )

    out, changed, changed_idx = auto_unpin_likely_wrong_provider_ids(df, registry=registry)
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
