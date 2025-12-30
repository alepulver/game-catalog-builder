from __future__ import annotations

import pandas as pd

from game_catalog_builder.metrics.registry import load_metrics_registry
from game_catalog_builder.utils.review import build_review_csv


def test_review_csv_uses_registry_columns_for_tags_confidence_and_matched_names(tmp_path) -> None:
    registry_yaml = tmp_path / "metrics.yaml"
    registry_yaml.write_text(
        "\n".join(
            [
                "version: 2",
                "metrics: {}",
                "diagnostics:",
                "  diagnostics.review.tags: { column: TagsX, type: string }",
                "  diagnostics.match.confidence: { column: ConfX, type: string }",
                "  diagnostics.rawg.matched_name: { column: RawgNameX, type: string }",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    reg = load_metrics_registry(registry_yaml)

    catalog_df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Doom",
                "ConfX": "HIGH",
                "TagsX": "provider_outlier:steam",
                "RawgNameX": "Doom (1993)",
            },
            {
                "RowId": "rid:2",
                "Name": "Quake",
                "ConfX": "HIGH",
                "TagsX": "",
                "RawgNameX": "Quake",
            },
        ]
    )

    out = build_review_csv(catalog_df, registry=reg)
    # Only the outlier-tag row should be kept when confidence is HIGH.
    assert len(out) == 1
    assert out.iloc[0]["RowId"] == "rid:1"
    # SuggestedTitle should come from the registry-mapped matched-name column.
    assert out.iloc[0]["SuggestedTitle"] == "Doom (1993)"
