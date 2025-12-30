from __future__ import annotations

from pathlib import Path

import pandas as pd

from game_catalog_builder.metrics.registry import load_metrics_registry
from game_catalog_builder.utils.signals import apply_phase1_signals


def test_phase1_signals_use_registry_metric_keys(tmp_path: Path) -> None:
    registry_path = tmp_path / "metrics.yaml"
    registry_path.write_text(
        "\n".join(
            [
                "version: 2",
                "metrics:",
                "  steam.review_count: { column: SteamReviewsX, type: int }",
                "  steamspy.owners: { column: SteamSpyOwnersX, type: string }",
                "  derived.reach.steam_reviews: { column: DerivedSteamReviewsX, type: int }",
                "  derived.reach.steamspy_owners_mid: { column: OwnersMidX, type: int }",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    tiers_path = tmp_path / "production_tiers.yaml"
    tiers_path.write_text("publishers: {}\ndevelopers: {}\n", encoding="utf-8")

    reg = load_metrics_registry(registry_path)

    df = pd.DataFrame(
        [
            {"RowId": "1", "SteamReviewsX": 1234, "SteamSpyOwnersX": "1,000 .. 2,000"},
            {"RowId": "2", "SteamReviewsX": "", "SteamSpyOwnersX": ""},
        ]
    )

    out = apply_phase1_signals(df, registry=reg, production_tiers_path=tiers_path)

    assert out.loc[out["RowId"] == "1", "DerivedSteamReviewsX"].iloc[0] == 1234
    assert out.loc[out["RowId"] == "1", "OwnersMidX"].iloc[0] == 1500
