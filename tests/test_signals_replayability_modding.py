from __future__ import annotations

import pandas as pd


def test_replayability_is_low_for_linear_single_player() -> None:
    from game_catalog_builder.utils.signals import apply_phase1_signals

    df = pd.DataFrame(
        [
            {
                "Name": "Linear Game",
                "Steam_Categories": ["Single-player", "Steam Achievements"],
                "IGDB_GameModes": ["Single player"],
                "HLTB_Main": "10",
                "HLTB_Extra": "11",
                "HLTB_Completionist": "12",
            }
        ]
    )

    out = apply_phase1_signals(df, production_tiers_path="data/does_not_exist.yaml")
    row = out.iloc[0].to_dict()

    assert isinstance(row["Replayability_100"], int)
    assert row["Replayability_100"] <= 30


def test_replayability_is_high_for_multiplayer() -> None:
    from game_catalog_builder.utils.signals import apply_phase1_signals

    df = pd.DataFrame(
        [
            {
                "Name": "MP Game",
                "Steam_Categories": ["Multi-player", "Online PvP"],
            }
        ]
    )

    out = apply_phase1_signals(df, production_tiers_path="data/does_not_exist.yaml")
    row = out.iloc[0].to_dict()
    assert int(row["Replayability_100"]) >= 70
    assert "multiplayer" in row["Replayability_SourceSignals"]


def test_replayability_boosts_for_roguelike_tag() -> None:
    from game_catalog_builder.utils.signals import apply_phase1_signals

    df = pd.DataFrame(
        [
            {
                "Name": "Rogue",
                "RAWG_Tags": ["Roguelike"],
            }
        ]
    )

    out = apply_phase1_signals(df, production_tiers_path="data/does_not_exist.yaml")
    row = out.iloc[0].to_dict()
    assert int(row["Replayability_100"]) >= 40
    assert "roguelike" in row["Replayability_SourceSignals"]


def test_replayability_uses_steamspy_tags() -> None:
    from game_catalog_builder.utils.signals import apply_phase1_signals

    df = pd.DataFrame([{"Name": "SteamSpy Tagged", "SteamSpy_Tags": ["Roguelike", "Survival"]}])
    out = apply_phase1_signals(df, production_tiers_path="data/does_not_exist.yaml")
    row = out.iloc[0].to_dict()
    assert int(row["Replayability_100"]) >= 40
    assert "roguelike" in row["Replayability_SourceSignals"]


def test_modding_signal_detects_workshop_category() -> None:
    from game_catalog_builder.utils.signals import apply_phase1_signals

    df = pd.DataFrame(
        [
            {
                "Name": "Workshop Game",
                "Steam_Categories": ["Single-player", "Steam Workshop"],
            }
        ]
    )

    out = apply_phase1_signals(df, production_tiers_path="data/does_not_exist.yaml")
    row = out.iloc[0].to_dict()
    assert row["HasWorkshop"] is True
    assert row["ModdingSignal_100"] == 90
    assert "steam_workshop" in row["Modding_SourceSignals"]


def test_modding_signal_is_zero_when_categories_present_but_no_signal() -> None:
    from game_catalog_builder.utils.signals import apply_phase1_signals

    df = pd.DataFrame(
        [
            {
                "Name": "No Mods",
                "Steam_Categories": ["Single-player", "Steam Achievements"],
            }
        ]
    )

    out = apply_phase1_signals(df, production_tiers_path="data/does_not_exist.yaml")
    row = out.iloc[0].to_dict()
    assert row["HasWorkshop"] == ""
    assert row["ModdingSignal_100"] == 0
    assert row["Modding_SourceSignals"] == ""
