from __future__ import annotations


def test_rawg_select_prefers_exact_base_title_over_sequel() -> None:
    from game_catalog_builder.clients.rawg_client import RAWGClient

    cands = [
        {"id": 1, "name": "Borderlands", "released": "2009-10-20"},
        {"id": 2, "name": "Borderlands 4", "released": "2025-01-01"},
    ]
    best, score, _top = RAWGClient._select_best_candidate(
        query="Borderlands", candidates=cands, year_hint=None
    )
    assert best is not None
    assert best["id"] == 1
    assert score >= 90


def test_rawg_select_avoids_demo_when_query_not_dlc_like() -> None:
    from game_catalog_builder.clients.rawg_client import RAWGClient

    cands = [
        {"id": 1, "name": "Trine 2 Demo", "released": "2011-01-01"},
        {"id": 2, "name": "Trine 2", "released": "2011-12-06"},
    ]
    best, _score, _top = RAWGClient._select_best_candidate(
        query="Trine 2", candidates=cands, year_hint=2011
    )
    assert best is not None
    assert best["id"] == 2


def test_rawg_select_uses_year_hint_to_prefer_release_near_year() -> None:
    from game_catalog_builder.clients.rawg_client import RAWGClient

    cands = [
        {"id": 1, "name": "Doom", "released": "2016-05-13"},
        {"id": 2, "name": "Doom", "released": "1993-12-10"},
    ]
    best, _score, _top = RAWGClient._select_best_candidate(
        query="Doom", candidates=cands, year_hint=1993
    )
    assert best is not None
    assert best["id"] == 2
