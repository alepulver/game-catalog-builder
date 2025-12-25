from __future__ import annotations

import pandas as pd

from game_catalog_builder.utils.signals import (
    apply_phase1_signals,
    compute_production_tier,
    parse_steamspy_owners_range,
)


def test_parse_steamspy_owners_range() -> None:
    assert parse_steamspy_owners_range("") == (None, None, None)
    assert parse_steamspy_owners_range("1,000,000 .. 2,000,000") == (
        1_000_000,
        2_000_000,
        1_500_000,
    )
    assert parse_steamspy_owners_range("2000..1000") == (1000, 2000, 1500)
    assert parse_steamspy_owners_range("not a range") == (None, None, None)


def test_compute_production_tier_prefers_publisher_then_developer() -> None:
    mapping = {
        "publishers": {"BigPub": "AAA"},
        "developers": {"SmallDev": "Indie"},
    }
    tier, reason = compute_production_tier(
        {"Steam_Publishers": '["Other","BigPub"]', "Steam_Developers": '["SmallDev"]'}, mapping
    )
    assert tier == "AAA"
    assert reason == "publisher:BigPub"


def test_apply_phase1_signals_adds_composites_and_reach_columns() -> None:
    df = pd.DataFrame(
        [
            {
                "Name": "Example",
                "SteamSpy_Owners": "1,000 .. 2,000",
                "SteamSpy_Positive": "90",
                "SteamSpy_Negative": "10",
                "Score_SteamSpy_100": "90",
                "SteamSpy_PlaytimeAvg2Weeks": "15",
                "SteamSpy_PlaytimeMedian2Weeks": "7",
                "RAWG_Rating": "4.0",
                "RAWG_RatingsCount": "100",
                "Score_IGDB_100": "80",
                "IGDB_RatingCount": "10",
                "Score_HLTB_100": "70",
                "Steam_ReviewCount": "1234",
                "Steam_Metacritic": "88",
                "RAWG_Metacritic": "84",
                "IGDB_AggregatedRating": "85",
                "IGDB_AggregatedRatingCount": "55",
                "Steam_Publishers": "",
                "Steam_Developers": "",
            }
        ]
    )

    out = apply_phase1_signals(df, production_tiers_path="data/does_not_exist.yaml")
    row = out.iloc[0].to_dict()

    assert row["Reach_SteamSpyOwners_Low"] == "1000"
    assert row["Reach_SteamSpyOwners_High"] == "2000"
    assert row["Reach_SteamSpyOwners_Mid"] == "1500"

    assert row["Reach_SteamReviews"] == "1234"
    assert row["Reach_RAWGRatingsCount"] == "100"
    assert row["Reach_IGDBRatingCount"] == "10"
    assert row["Reach_IGDBAggregatedRatingCount"] == "55"

    assert row["Now_SteamSpyPlaytimeAvg2Weeks"] == "15"
    assert row["Now_SteamSpyPlaytimeMedian2Weeks"] == "7"

    # sanity: composite fields exist and are numeric-ish
    assert row["CommunityRating_Composite_100"].isdigit()
    assert row["CriticRating_Composite_100"].isdigit()
