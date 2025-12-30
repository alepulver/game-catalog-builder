from __future__ import annotations


def test_igdb_select_prefers_main_game_category_over_dlc() -> None:
    from game_catalog_builder.clients.igdb_client import IGDBClient

    results = [
        {"id": 1, "name": "Example", "category": 1, "first_release_date": 946684800},
        {"id": 2, "name": "Example", "category": 0, "first_release_date": 946684800},
    ]
    best, _score, _top = IGDBClient._select_best_match(query="Example", results=results, year_hint=2000)
    assert best is not None
    assert best["id"] == 2


def test_igdb_select_prefers_exact_title_over_numbered_variant() -> None:
    from game_catalog_builder.clients.igdb_client import IGDBClient

    results = [
        {"id": 1, "name": "Diablo IV", "category": 0, "first_release_date": 1685577600},
        {"id": 2, "name": "Diablo", "category": 0, "first_release_date": 725846400},
    ]
    best, _score, _top = IGDBClient._select_best_match(query="Diablo", results=results, year_hint=None)
    assert best is not None
    assert best["id"] == 2


def test_igdb_select_uses_year_hint_to_prefer_release_near_year() -> None:
    from game_catalog_builder.clients.igdb_client import IGDBClient

    results = [
        {"id": 1, "name": "Doom", "category": 0, "first_release_date": 1463097600},  # 2016
        {"id": 2, "name": "Doom", "category": 0, "first_release_date": 755827200},  # 1993
    ]
    best, _score, _top = IGDBClient._select_best_match(query="Doom", results=results, year_hint=1993)
    assert best is not None
    assert best["id"] == 2
