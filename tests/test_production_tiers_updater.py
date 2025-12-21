from __future__ import annotations

from pathlib import Path

from game_catalog_builder.tools.production_tiers_updater import (
    WikipediaClient,
    WikipediaPick,
    pick_wikipedia_page,
    suggest_and_update_production_tiers,
    suggest_tier_from_wikipedia_extract,
    update_production_tiers_yaml_in_place,
)


class _StubWiki(WikipediaClient):
    def __init__(
        self,
        *,
        opensearch_map: dict[str, list[tuple[str, str]]],
        summaries: dict[str, str],
    ):
        # Avoid initializing requests/session/caches
        self._opensearch_map = opensearch_map
        self._summaries = summaries

    def opensearch(self, query: str, *, limit: int = 10) -> list[tuple[str, str]]:  # type: ignore[override]
        return list(self._opensearch_map.get(query, []))[:limit]

    def search(self, query: str, *, limit: int = 10) -> list[str]:  # type: ignore[override]
        # Return titles only from opensearch map for simplicity.
        return [t for t, _ in self.opensearch(query, limit=limit)]

    def summary(self, title: str) -> tuple[str, str]:  # type: ignore[override]
        return (self._summaries.get(title, ""), f"https://example.test/wiki/{title}")


def test_suggest_tier_from_extract_indie() -> None:
    tier, reason = suggest_tier_from_wikipedia_extract(
        "Foo is an independent video game developer based in Somewhere.", entity_type="developer"
    )
    assert tier == "Indie"
    assert "independent" in reason


def test_suggest_tier_from_extract_owned_by_major() -> None:
    tier, reason = suggest_tier_from_wikipedia_extract(
        "Bar is a video game developer and a subsidiary of Sega.", entity_type="developer"
    )
    assert tier == "AAA"
    assert "owned_by_major" in reason


def test_pick_wikipedia_page_prefers_game_company_extract() -> None:
    client = _StubWiki(
        opensearch_map={
            "Guard Crush": [
                ("Guard Corps (Haganah)", "u1"),
                ("Guard Crush Games", "u2"),
            ]
        },
        summaries={
            "Guard Corps (Haganah)": "Heil Mishmar was the guard corps of the Haganah...",
            "Guard Crush Games": "Guard Crush Games is an independent video game developer.",
        },
    )
    pick = pick_wikipedia_page(client=client, entity_name="Guard Crush", entity_type="developer")
    assert isinstance(pick, WikipediaPick)
    assert pick.title == "Guard Crush Games"


def test_update_production_tiers_yaml_in_place_preserves_structure(tmp_path: Path) -> None:
    p = tmp_path / "tiers.yaml"
    p.write_text(
        (
            "# header\n"
            "publishers:\n"
            '  "Existing Pub": "AAA"\n'
            "\n"
            "developers:\n"
            '  "Existing Dev": "Indie"\n'
        ),
        encoding="utf-8",
    )
    update_production_tiers_yaml_in_place(
        p,
        add_publishers={"New Pub": "AA"},
        add_developers={"New Dev": "AAA"},
    )
    text = p.read_text(encoding="utf-8")
    assert 'publishers:' in text
    assert 'developers:' in text
    assert "Existing Pub:" in text
    assert "New Pub:" in text
    assert "Existing Dev:" in text
    assert "New Dev:" in text


def test_update_production_tiers_yaml_in_place_valid_yaml(tmp_path: Path) -> None:
    p = tmp_path / "tiers.yaml"
    p.write_text("publishers:\ndevelopers:\n", encoding="utf-8")
    update_production_tiers_yaml_in_place(
        p,
        add_publishers={"A \"Quoted\" Pub": "AAA"},
        add_developers={},
    )
    # If it parses, quoting worked.
    import yaml

    parsed = yaml.safe_load(p.read_text(encoding="utf-8"))
    assert parsed["publishers"]["A \"Quoted\" Pub"] == "AAA"


def test_suggest_and_update_can_ensure_complete_with_defaults(tmp_path: Path) -> None:
    enriched = tmp_path / "enriched.csv"
    enriched.write_text(
        "RowId,Name,Steam_Publishers,Steam_Developers\n"
        '1,Game A,["Pub A"],["Dev A"]\n'
        '2,Game B,["Pub B"],["Dev B"]\n',
        encoding="utf-8",
    )
    yaml_path = tmp_path / "tiers.yaml"
    yaml_path.write_text("publishers:\ndevelopers:\n", encoding="utf-8")

    client = _StubWiki(opensearch_map={}, summaries={})
    res = suggest_and_update_production_tiers(
        enriched_csv=enriched,
        yaml_path=yaml_path,
        wiki_cache_path=tmp_path / "wiki.json",
        apply=True,
        max_items=0,
        min_count=1,
        ensure_complete=True,
        unknown_tier="Unknown",
        wiki_client=client,
        include_known_seeds=False,
    )
    assert res.added_publishers == 2
    assert res.added_developers == 2
    import yaml

    parsed = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    assert parsed["publishers"]["Pub A"] == "Unknown"
    assert parsed["publishers"]["Pub B"] == "Unknown"
    assert parsed["developers"]["Dev A"] == "Unknown"
    assert parsed["developers"]["Dev B"] == "Unknown"
