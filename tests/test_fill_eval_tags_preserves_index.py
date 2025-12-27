from __future__ import annotations

import pandas as pd


def test_fill_eval_tags_writes_tags_with_original_index() -> None:
    from game_catalog_builder.analysis.import_diagnostics import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Halo 2",
                "Platform": "PC",
                "Steam_AppID": "",
            }
        ],
        index=[123],
    )

    out = fill_eval_tags(df, sources={"steam"})
    assert out.index.tolist() == [123]
    assert out.loc[123, "ReviewTags"] == "missing_steam"
    assert out.loc[123, "MatchConfidence"] == "MEDIUM"
