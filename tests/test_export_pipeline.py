from __future__ import annotations

import json
from pathlib import Path


def test_export_enriched_csv_from_jsonl(tmp_path: Path) -> None:
    from game_catalog_builder.pipelines.export_pipeline import export_enriched_csv

    run_dir = tmp_path / "run"
    (run_dir / "output" / "jsonl").mkdir(parents=True)
    (run_dir / "metrics-registry.example.yaml").write_text(
        Path("data/metrics-registry.example.yaml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    jsonl = run_dir / "output" / "jsonl" / "Games_Enriched.jsonl"
    # Export relies on the JSONL manifest to preserve the exact column set/order (including
    # empty columns that are omitted from JSONL rows for compactness).
    (run_dir / "output" / "jsonl" / "Games_Enriched.manifest.json").write_text(
        json.dumps(
            {
                "columns": [
                    "RowId",
                    "Name",
                    "Notes",
                    "Play again",
                    "Steam_AppID",
                    "Reach_Composite",
                    "RAWG_Added",
                    "SteamSpy_TagsTop",
                ]
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    rows = [
        {
            "row_id": "1",
            "personal": {"Name": "Example", "Notes": "Hi"},
            "pins": {"Steam_AppID": "620"},
            "metrics": {
                "composite.reach.score_100": 42,
                "rawg.popularity.added_total": 1000,
                "steamspy.popularity.tags_top": [["FPS", 10]],
            },
            "diagnostics": {},
            "meta": {},
        }
    ]
    jsonl.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8")

    out_csv = run_dir / "output" / "csv" / "Games_Enriched.csv"
    export_enriched_csv(run_dir=run_dir, input_jsonl=jsonl, output_csv=out_csv, metrics_registry=None)

    text = out_csv.read_text(encoding="utf-8")
    header = text.splitlines()[0]
    assert header == ("RowId,Name,Notes,Play again,Steam_AppID,Reach_Composite,RAWG_Added,SteamSpy_TagsTop")
    # Play again is intentionally missing from the JSONL row; it should still exist in CSV.
    assert text.splitlines()[1].split(",")[3] == ""


def test_export_enriched_json_from_jsonl(tmp_path: Path) -> None:
    from game_catalog_builder.pipelines.export_pipeline import export_enriched_json

    run_dir = tmp_path / "run"
    (run_dir / "output" / "jsonl").mkdir(parents=True)
    (run_dir / "metrics-registry.example.yaml").write_text(
        Path("data/metrics-registry.example.yaml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    jsonl = run_dir / "output" / "jsonl" / "Games_Enriched.jsonl"
    (run_dir / "output" / "jsonl" / "Games_Enriched.manifest.json").write_text(
        json.dumps({"columns": ["RowId", "Name", "SteamSpy_TagsTop"]}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    jsonl.write_text(
        json.dumps(
            {
                "row_id": "1",
                "personal": {"Name": "Example"},
                "pins": {},
                "metrics": {"steamspy.popularity.tags_top": [["FPS", 10]]},
                "diagnostics": {},
                "meta": {},
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    out_json = run_dir / "output" / "json" / "Games_Enriched.json"
    export_enriched_json(run_dir=run_dir, input_jsonl=jsonl, output_json=out_json, metrics_registry=None)
    obj = json.loads(out_json.read_text(encoding="utf-8"))
    assert obj == [{"RowId": "1", "Name": "Example", "SteamSpy_TagsTop": [["FPS", 10]]}]


def test_export_uses_manifest_column_to_key_without_registry_mapping(tmp_path: Path) -> None:
    """
    When JSONL was produced with --all-metrics, some columns may be stored under auto keys that
    are not present in the registry YAML. Export must rely on the per-run manifest mapping.
    """
    from game_catalog_builder.pipelines.export_pipeline import export_enriched_csv

    run_dir = tmp_path / "run"
    (run_dir / "output" / "jsonl").mkdir(parents=True)
    # Minimal registry that does NOT define RAWG_Added.
    (run_dir / "metrics-registry.example.yaml").write_text(
        "\n".join(
            [
                "version: 2",
                "metrics:",
                "  composite.reach.score_100: { column: Reach_Composite, type: int }",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    jsonl = run_dir / "output" / "jsonl" / "Games_Enriched.jsonl"
    (run_dir / "output" / "jsonl" / "Games_Enriched.manifest.json").write_text(
        json.dumps(
            {
                "columns": ["RowId", "Name", "RAWG_Added", "Reach_Composite"],
                "column_to_metric_key": {
                    "RAWG_Added": "rawg.added",
                    "Reach_Composite": "composite.reach.score_100",
                },
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    jsonl.write_text(
        json.dumps(
            {
                "row_id": "1",
                "personal": {"Name": "Example"},
                "pins": {},
                "metrics": {"rawg.added": 1000, "composite.reach.score_100": 42},
                "diagnostics": {},
                "meta": {},
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    out_csv = run_dir / "output" / "csv" / "Games_Enriched.csv"
    export_enriched_csv(run_dir=run_dir, input_jsonl=jsonl, output_csv=out_csv, metrics_registry=None)
    lines = out_csv.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "RowId,Name,RAWG_Added,Reach_Composite"
    assert lines[1] == "1,Example,1000,42"


def test_sync_updates_internal_jsonl_personal_and_pins(tmp_path: Path) -> None:
    from game_catalog_builder.pipelines.sync_pipeline import sync_back_catalog

    catalog = tmp_path / "Games_Catalog.csv"
    catalog.write_text("RowId,Name,Notes,Steam_AppID\n1,Old,Old note,620\n", encoding="utf-8")

    enriched = tmp_path / "Games_Enriched.csv"
    enriched.write_text("RowId,Name,Notes,Steam_AppID\n1,New,New note,999\n", encoding="utf-8")

    internal = tmp_path / "output" / "jsonl" / "Games_Enriched.jsonl"
    internal.parent.mkdir(parents=True, exist_ok=True)
    internal.write_text(
        json.dumps(
            {
                "row_id": "1",
                "personal": {"Name": "Old", "Notes": "Old note"},
                "pins": {"Steam_AppID": "620"},
                "metrics": {"composite.reach.score_100": 10},
                "diagnostics": {},
                "meta": {},
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    out_catalog = tmp_path / "out.csv"
    sync_back_catalog(
        catalog_csv=catalog,
        enriched_csv=enriched,
        output_csv=out_catalog,
        internal_jsonl=internal,
    )

    obj = json.loads(internal.read_text(encoding="utf-8").splitlines()[0])
    assert obj["personal"]["Name"] == "New"
    assert obj["personal"]["Notes"] == "New note"
    assert obj["pins"]["Steam_AppID"] == "999"
    # Metrics remain unchanged by sync.
    assert obj["metrics"]["composite.reach.score_100"] == 10


def test_sync_accepts_enriched_json(tmp_path: Path) -> None:
    from game_catalog_builder.pipelines.sync_pipeline import sync_back_catalog

    catalog = tmp_path / "Games_Catalog.csv"
    catalog.write_text("RowId,Name,Notes,Steam_AppID\n1,Old,Old note,620\n", encoding="utf-8")

    enriched_json = tmp_path / "Games_Enriched.json"
    enriched_json.write_text(
        json.dumps([{"RowId": "1", "Name": "New", "Notes": "New note", "Steam_AppID": "999"}]) + "\n",
        encoding="utf-8",
    )

    internal = tmp_path / "output" / "jsonl" / "Games_Enriched.jsonl"
    internal.parent.mkdir(parents=True, exist_ok=True)
    internal.write_text(
        json.dumps(
            {
                "row_id": "1",
                "personal": {"Name": "Old", "Notes": "Old note"},
                "pins": {"Steam_AppID": "620"},
                "metrics": {"composite.reach.score_100": 10},
                "diagnostics": {},
                "meta": {},
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    out_catalog = tmp_path / "out.csv"
    sync_back_catalog(
        catalog_csv=catalog,
        enriched_csv=enriched_json,
        output_csv=out_catalog,
        internal_jsonl=internal,
    )

    obj = json.loads(internal.read_text(encoding="utf-8").splitlines()[0])
    assert obj["personal"]["Name"] == "New"
    assert obj["pins"]["Steam_AppID"] == "999"
