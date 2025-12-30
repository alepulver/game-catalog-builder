from __future__ import annotations

import pandas as pd

from game_catalog_builder.metrics.registry import MetricsRegistry
from game_catalog_builder.pipelines.import_pipeline import _match_rawg_ids


class DummyRAWG:
    def get_by_id(self, rawg_id: str) -> dict[str, object]:
        return {"id": int(rawg_id), "name": "Doom", "released": "1993-12-10"}

    def search(self, name: str, year_hint: int | None = None) -> dict[str, object]:
        assert name == "Doom"
        return {"id": 123, "name": "Doom", "released": "1993-12-10"}

    def format_cache_stats(self) -> str:
        return ""


def test_import_diagnostics_are_mapped_via_registry_not_hardcoded() -> None:
    registry = MetricsRegistry(
        by_key={},
        by_column={},
        diagnostics_by_key={
            "diagnostics.rawg.matched_name": ("X_RAWG_MATCHED", "string"),
            "diagnostics.rawg.match_score": ("X_RAWG_SCORE", "int"),
            "diagnostics.rawg.matched_year": ("X_RAWG_YEAR", "int"),
        },
        diagnostics_by_column={
            "X_RAWG_MATCHED": ("diagnostics.rawg.matched_name", "string"),
            "X_RAWG_SCORE": ("diagnostics.rawg.match_score", "int"),
            "X_RAWG_YEAR": ("diagnostics.rawg.matched_year", "int"),
        },
    )

    df = pd.DataFrame([{"RowId": "rid:1", "Name": "Doom", "RAWG_ID": ""}])
    _match_rawg_ids(
        df,
        client=DummyRAWG(),
        include_diagnostics=True,
        registry=registry,
        active_total=1,
    )

    assert df.at[0, "RAWG_ID"] == "123"
    assert df.at[0, "X_RAWG_MATCHED"] == "Doom"
    assert int(df.at[0, "X_RAWG_SCORE"]) == 100
    assert int(df.at[0, "X_RAWG_YEAR"]) == 1993
    assert "RAWG_MatchedName" not in df.columns
