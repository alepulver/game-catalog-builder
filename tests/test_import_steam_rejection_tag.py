from __future__ import annotations

import pandas as pd


def test_fill_eval_tags_adds_steam_rejected_tag_when_reason_present() -> None:
    from game_catalog_builder.pipelines.diagnostics.import_diagnostics import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Example",
                "Platform": "PC",
                "Steam_AppID": "",
                "Steam_RejectedReason": "non_game:advertising",
                "Disabled": "",
            }
        ]
    )
    out = fill_eval_tags(df, sources={"steam"})
    tags = out.loc[0, "ReviewTags"]
    assert "missing_steam" in tags
    assert "steam_rejected" in tags
    assert "steam_rejected:non_game:advertising" in tags
