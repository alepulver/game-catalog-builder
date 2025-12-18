from __future__ import annotations


def test_pick_best_match_prefers_exact_over_year_variant() -> None:
    from game_catalog_builder.utils.utilities import pick_best_match

    candidates = [
        {"id": 2454, "name": "DOOM (2016)"},
        {"id": 2280, "name": "DOOM"},
    ]
    best, score, _ = pick_best_match("Doom", candidates, name_key="name")
    assert best is not None
    assert best["name"] == "DOOM"
    assert score == 100


def test_pick_best_match_prefers_year_variant_when_query_includes_year() -> None:
    from game_catalog_builder.utils.utilities import pick_best_match

    candidates = [
        {"id": 2454, "name": "DOOM (2016)"},
        {"id": 2280, "name": "DOOM"},
    ]
    best, score, _ = pick_best_match("Doom (2016)", candidates, name_key="name")
    assert best is not None
    assert best["name"] == "DOOM (2016)"
    assert score == 100


def test_pick_best_match_allows_numbered_prefix_subtitle() -> None:
    from game_catalog_builder.utils.utilities import pick_best_match

    candidates = [
        {"id": 1, "name": "POSTAL 4: No Regerts"},
        {"id": 2, "name": "Postal 2"},
    ]
    best, score, _ = pick_best_match("Postal 4", candidates, name_key="name")
    assert best is not None
    assert best["name"] == "POSTAL 4: No Regerts"
    assert score >= 65


def test_pick_best_match_avoids_demo_when_full_game_available() -> None:
    from game_catalog_builder.utils.utilities import pick_best_match

    candidates = [
        {"id": 204260, "name": "Trine 2 Demo"},
        {"id": 35700, "name": "Trine 2"},
    ]
    best, score, _ = pick_best_match("Trine 2", candidates, name_key="name")
    assert best is not None
    assert best["name"] == "Trine 2"
    assert score == 100


def test_pick_best_match_uses_year_hint_as_soft_tiebreak() -> None:
    from game_catalog_builder.utils.utilities import pick_best_match

    candidates = [
        {"id": 1, "name": "Doom", "released": "1993-12-10"},
        {"id": 2, "name": "Doom", "released": "2016-05-13"},
    ]

    def year_getter(obj):
        return int(str(obj.get("released", ""))[:4])

    best, score, _ = pick_best_match(
        "Doom", candidates, name_key="name", year_hint=1993, year_getter=year_getter
    )
    assert best is not None
    assert best["id"] == 1
    assert score == 100
