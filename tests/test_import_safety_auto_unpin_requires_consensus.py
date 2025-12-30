from __future__ import annotations

import pandas as pd


def test_auto_unpin_requires_consensus_and_outlier_tag():
    from game_catalog_builder.metrics.registry import MetricsRegistry
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
                # has consensus + outlier + likely_wrong -> should unpin
                "ReviewTags": "provider_consensus:igdb+rawg+hltb, provider_outlier:steam, "
                "year_outlier:steam, likely_wrong:steam",
                "MatchConfidence": "LOW",
            },
        ]
    )

    out, changed, changed_idx = auto_unpin_likely_wrong_provider_ids(df, registry=registry)
    assert changed == 1
    assert changed_idx == [1]
    assert out.loc[0, "Steam_AppID"] == "111"
    assert out.loc[1, "Steam_AppID"] == ""
    assert "autounpinned:steam" in out.loc[1, "ReviewTags"]
