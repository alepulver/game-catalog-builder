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
        "publishers": {"bigpub": {"tier": "AAA", "label": "BigPub", "source": "test"}},
        "developers": {"smalldev": {"tier": "Indie", "label": "SmallDev", "source": "test"}},
    }
    tier, reason = compute_production_tier(
        {"steam.publishers": ["Other", "BigPub"], "steam.developers": ["SmallDev"]}, mapping
    )
    assert tier == "AAA"
    assert reason == "publisher:BigPub"


def test_compute_production_tier_uses_non_steam_company_fields() -> None:
    mapping = {
        "publishers": {"rawgpub": {"tier": "AA", "label": "RAWGPub", "source": "test"}},
        "developers": {},
    }
    tier, reason = compute_production_tier({"rawg.publishers": ["RAWGPub"]}, mapping)
    assert tier == "AA"
    assert reason == "publisher:RAWGPub"


def test_compute_production_tier_returns_unknown_when_company_present_but_unmapped() -> None:
    mapping = {"publishers": {}, "developers": {}}
    tier, reason = compute_production_tier({"igdb.developers": ["Some Studio"]}, mapping)
    assert tier in {"Unknown", ""}
    assert reason in {"", "developer:Some Studio"}


def test_apply_phase1_signals_adds_composites_and_reach_columns() -> None:
    df = pd.DataFrame(
        [
            {
                "Name": "Example",
                "SteamSpy_Owners": "1,000 .. 2,000",
                "SteamSpy_Positive": 90,
                "SteamSpy_Negative": 10,
                "SteamSpy_Score_100": 90,
                "SteamSpy_PlaytimeAvg2Weeks": 15,
                "SteamSpy_PlaytimeMedian2Weeks": 7,
                "RAWG_Score_100": 80,
                "RAWG_RatingsCount": 100,
                "IGDB_Score_100": 80,
                "IGDB_ScoreCount": 10,
                "HLTB_Score_100": 70,
                "Steam_ReviewCount": 1234,
                "Steam_Metacritic": 88,
                "RAWG_Metacritic": 84,
                "IGDB_CriticScoreCount": 55,
                "IGDB_CriticScore_100": 85,
                "Steam_Publishers": "",
                "Steam_Developers": "",
            }
        ]
    )

    out = apply_phase1_signals(df, production_tiers_path="data/does_not_exist.json")
    row = out.iloc[0].to_dict()

    assert row["Reach_SteamSpyOwners_Low"] == 1000
    assert row["Reach_SteamSpyOwners_High"] == 2000
    assert row["Reach_SteamSpyOwners_Mid"] == 1500

    assert row["Reach_SteamReviews"] == 1234
    assert row["Reach_RAWGRatingsCount"] == 100
    assert row["Reach_IGDBRatingCount"] == 10
    assert row["Reach_IGDBAggregatedRatingCount"] == 55

    assert row["Now_SteamSpyPlaytimeAvg2Weeks"] == 15
    assert row["Now_SteamSpyPlaytimeMedian2Weeks"] == 7

    # sanity: composite fields exist and are numeric-ish
    assert isinstance(row["CommunityRating_Composite_100"], int)
    assert isinstance(row["CriticRating_Composite_100"], int)
    # New consensus columns default to empty when not enough provider data exists.
    assert row["Developers_ConsensusProviders"] == ""
    assert row["Publishers_ConsensusProviders"] == ""


def test_apply_phase1_signals_adds_content_type_consensus_from_steam() -> None:
    from game_catalog_builder.utils.signals import apply_phase1_signals

    df = pd.DataFrame([{"Name": "Example DLC", "Steam_StoreType": "dlc"}])
    out = apply_phase1_signals(df, production_tiers_path="data/does_not_exist.json")
    row = out.iloc[0].to_dict()
    assert row["ContentType"] == "dlc"
    assert row["ContentType_ConsensusProviders"] == "steam"
    assert "steam:type=dlc" in row["ContentType_SourceSignals"]
    assert row["ContentType_Conflict"] == ""


def test_apply_phase1_signals_content_type_is_empty_when_no_consensus() -> None:
    from game_catalog_builder.utils.signals import apply_phase1_signals

    df = pd.DataFrame(
        [
            {
                "Name": "Example",
                "Steam_StoreType": "game",
                "IGDB_VersionParent": "Example (1993)",
            }
        ]
    )
    out = apply_phase1_signals(df, production_tiers_path="data/does_not_exist.json")
    row = out.iloc[0].to_dict()
    assert row["ContentType"] == ""
    assert row["ContentType_ConsensusProviders"] == ""
    assert row["ContentType_Conflict"] == "YES"


def test_has_dlcs_expansions_ports_derived_from_igdb_lists_and_filters_soundtracks() -> None:
    from game_catalog_builder.utils.signals import apply_phase1_signals

    df = pd.DataFrame(
        [
            {
                "Name": "Example",
                "IGDB_DLCs": ["Example - Soundtrack", "Example Artbook", "Example DLC 1"],
                "IGDB_Expansions": ["Example Expansion"],
                "IGDB_Ports": ["Example (Switch)"],
            }
        ]
    )
    out = apply_phase1_signals(df, production_tiers_path="data/does_not_exist.json")
    row = out.iloc[0].to_dict()
    assert row["HasDLCs"] is True
    assert row["HasExpansions"] is True
    assert row["HasPorts"] is True
    # Filtered counts appear in the source signals.
    assert "igdb:dlcs=1" in row["ContentType_SourceSignals"]
    assert "igdb:expansions=1" in row["ContentType_SourceSignals"]
    assert "igdb:ports=1" in row["ContentType_SourceSignals"]
