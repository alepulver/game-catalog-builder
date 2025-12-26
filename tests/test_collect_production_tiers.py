from __future__ import annotations

from pathlib import Path

import yaml

from game_catalog_builder.tools.collect_production_tiers import collect_production_tiers_yaml


def test_collect_production_tiers_yaml_writes_tiers_only_and_preserves_tier(tmp_path: Path) -> None:
    enriched = tmp_path / "enriched.csv"
    enriched.write_text(
        "RowId,Name,Steam_Publishers,Steam_Developers\n"
        '1,Game A,["Pub A"],["Dev A"]\n'
        '2,Game B,["Pub A"],["Dev B"]\n',
        encoding="utf-8",
    )
    out_yaml = tmp_path / "production_tiers.yaml"
    out_yaml.write_text(
        yaml.safe_dump(
            {"publishers": {"Pub A": {"tier": "AAA"}}, "developers": {"Dev A": {"tier": "Indie"}}},
            sort_keys=False,
            allow_unicode=True,
            width=100,
        ),
        encoding="utf-8",
    )

    res = collect_production_tiers_yaml(enriched_csv=enriched, out_yaml=out_yaml, max_examples=2)
    assert res.publishers_total == 1
    assert res.developers_total == 1

    parsed = yaml.safe_load(out_yaml.read_text(encoding="utf-8"))
    assert parsed["publishers"]["Pub A"] == "AAA"
    assert parsed["developers"]["Dev A"] == "Indie"
    assert "Dev B" not in parsed["developers"]


def test_collect_production_tiers_only_missing_filters_filled(tmp_path: Path) -> None:
    enriched = tmp_path / "enriched.csv"
    enriched.write_text(
        "RowId,Name,Steam_Publishers,Steam_Developers\n"
        '1,Game A,["Pub A"],["Dev A"]\n'
        '2,Game B,["Pub B"],["Dev B"]\n',
        encoding="utf-8",
    )
    out_yaml = tmp_path / "production_tiers.yaml"
    out_yaml.write_text(
        yaml.safe_dump(
            {"publishers": {"Pub A": {"tier": "AAA"}}, "developers": {"Dev A": {"tier": "Indie"}}},
            sort_keys=False,
            allow_unicode=True,
            width=100,
        ),
        encoding="utf-8",
    )

    collect_production_tiers_yaml(
        enriched_csv=enriched, out_yaml=out_yaml, max_examples=1, keep_existing=True, only_missing=True
    )
    parsed = yaml.safe_load(out_yaml.read_text(encoding="utf-8"))
    assert "Pub A" not in parsed["publishers"]
    assert "Dev A" not in parsed["developers"]
    assert "Pub B" in parsed["publishers"]
    assert "Dev B" in parsed["developers"]
