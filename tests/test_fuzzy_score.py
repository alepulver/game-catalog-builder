from __future__ import annotations


def test_fuzzy_score_avoids_substring_false_positives():
    from game_catalog_builder.utils.utilities import fuzzy_score

    assert fuzzy_score("60 Seconds!", "60 Seconds Santa Run") < 90


def test_fuzzy_score_allows_year_only_expansion():
    from game_catalog_builder.utils.utilities import fuzzy_score

    assert fuzzy_score("Doom", "Doom (2016)") == 100
