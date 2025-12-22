from __future__ import annotations

import pandas as pd


def test_build_review_csv_prioritizes_low_confidence() -> None:
    from game_catalog_builder.utils.review import ReviewConfig, build_review_csv

    catalog = pd.DataFrame(
        [
            {"RowId": "rid:1", "Name": "A", "MatchConfidence": "HIGH", "ReviewTags": ""},
            {
                "RowId": "rid:2",
                "Name": "B",
                "MatchConfidence": "LOW",
                "ReviewTags": "missing_steam",
            },
            {
                "RowId": "rid:3",
                "Name": "C",
                "MatchConfidence": "MEDIUM",
                "ReviewTags": "year_outlier:steam",
            },
        ]
    )
    out = build_review_csv(catalog, config=ReviewConfig(max_rows=10))
    assert out.loc[0, "RowId"] == "rid:2"
    assert out.loc[1, "RowId"] == "rid:3"


def test_build_review_csv_includes_enriched_context() -> None:
    from game_catalog_builder.utils.review import ReviewConfig, build_review_csv

    catalog = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Example",
                "MatchConfidence": "LOW",
                "ReviewTags": "missing_steam",
                "Steam_AppID": "",
            }
        ]
    )
    enriched = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Wikidata_Wikipedia": "https://en.wikipedia.org/wiki/Example",
                "Wikidata_WikipediaSummary": "x" * 400,
            }
        ]
    )
    out = build_review_csv(catalog, enriched_df=enriched, config=ReviewConfig(max_rows=10))
    assert "Wikidata_Wikipedia" in out.columns
    assert out.loc[0, "Wikidata_Wikipedia"].startswith("https://")
    assert out.loc[0, "Wikidata_WikipediaSummary"].endswith("…")
