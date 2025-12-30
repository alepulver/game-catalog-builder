from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def test_jsonl_writer_uses_stable_envelope_and_registry_types(tmp_path: Path) -> None:
    from game_catalog_builder.metrics.jsonl import write_dataframe_jsonl
    from game_catalog_builder.metrics.registry import load_metrics_registry

    registry = load_metrics_registry(Path("data/metrics-registry.example.yaml"))

    df = pd.DataFrame(
        [
            {
                "RowId": "1",
                "Name": "Example",
                "Steam_AppID": "620",
                "RAWG_Added": 1000,
                "RAWG_AddedByStatusOwned": 900,
                "SteamSpy_TagsTop": [["Roguelike", 10], ["FPS", 3]],
                "Reach_Composite": 42,
                "HasWorkshop": True,
                "Replayability_100": 80,
            }
        ]
    )

    out = tmp_path / "out.jsonl"
    write_dataframe_jsonl(df, path=out, registry=registry, include_diagnostics=False)

    line = out.read_text(encoding="utf-8").splitlines()[0]
    obj = json.loads(line)

    assert obj["row_id"] == "1"
    assert obj["personal"]["Name"] == "Example"
    assert obj["pins"]["Steam_AppID"] == "620"

    metrics = obj["metrics"]
    # Column-rule mapping + typing
    assert metrics["rawg.popularity.added_total"] == 1000
    assert metrics["rawg.popularity.added_by_status.owned"] == 900
    assert metrics["steamspy.popularity.tags_top"] == [["Roguelike", 10], ["FPS", 3]]
    # Explicit mapping + typing
    assert metrics["composite.reach.score_100"] == 42
    assert metrics["derived.replayability.score_100"] == 80
    assert metrics["derived.modding.has_workshop"] is True
