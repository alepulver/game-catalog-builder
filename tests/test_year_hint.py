from __future__ import annotations


def test_extract_year_hint_parses_common_patterns():
    from game_catalog_builder.utils.utilities import extract_year_hint

    assert extract_year_hint("Spider-Man 2 (2004)") == 2004
    assert extract_year_hint("Silent Hill (1999)") == 1999
    assert extract_year_hint("Alien vs Predator 2010") == 2010


def test_extract_year_hint_ignores_non_year_numbers():
    from game_catalog_builder.utils.utilities import extract_year_hint

    assert extract_year_hint("007 Legends") is None
    assert extract_year_hint("NBA 2K24") is None
    assert extract_year_hint("") is None
