from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def test_unmapped_metric_columns_are_omitted_from_jsonl_and_export(tmp_path: Path) -> None:
    """
    Registry acts as a selection mechanism:
    - metric candidate columns (provider/derived) not mapped by the registry are omitted
    - personal columns remain preserved
    - JSONL manifest controls exported CSV columns/order
    """
    from game_catalog_builder.metrics.jsonl import write_dataframe_jsonl
    from game_catalog_builder.metrics.registry import load_metrics_registry
    from game_catalog_builder.pipelines.export_pipeline import export_enriched_csv

    registry_yaml = tmp_path / "metrics-registry.yaml"
    registry_yaml.write_text(
        "\n".join(
            [
                "version: 2",
                "metrics:",
                "  composite.reach.score_100: { column: Reach_Composite, type: int }",
                # Deliberately do NOT include RAWG_Added.
                "  steamspy.popularity.tags_top: { column: SteamSpy_TagsTop, type: json }",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    registry = load_metrics_registry(registry_yaml)

    df = pd.DataFrame(
        [
            {
                "RowId": "1",
                "Name": "Example",
                "Notes": "Hi",
                "Reach_Composite": 42,
                "RAWG_Added": 1000,
                "SteamSpy_TagsTop": [["FPS", 10]],
            }
        ]
    )

    run_dir = tmp_path / "run"
    jsonl = run_dir / "output" / "jsonl" / "Games_Enriched.jsonl"
    write_dataframe_jsonl(df, path=jsonl, registry=registry, include_diagnostics=False)

    # JSONL metrics omit unmapped RAWG_Added.
    obj = json.loads(jsonl.read_text(encoding="utf-8").splitlines()[0])
    assert "rawg.popularity.added_total" not in obj["metrics"]

    # Exported CSV omits the RAWG_Added column entirely (selection via manifest).
    out_csv = run_dir / "output" / "csv" / "Games_Enriched.csv"
    export_enriched_csv(
        run_dir=run_dir, input_jsonl=jsonl, output_csv=out_csv, metrics_registry=registry_yaml
    )
    header = out_csv.read_text(encoding="utf-8").splitlines()[0].split(",")
    assert "RAWG_Added" not in header
    # Personal columns are preserved.
    assert "Notes" in header
