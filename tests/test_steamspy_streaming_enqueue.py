from __future__ import annotations

from pathlib import Path

import pandas as pd


def test_steamspy_streaming_enqueues_existing_appids(tmp_path: Path, monkeypatch):
    """
    Regression test: if Steam_AppID is already present in Provider_Steam.csv, the streaming pipeline
    must still enqueue those rows so SteamSpy can populate Provider_SteamSpy.csv.
    """
    from game_catalog_builder.metrics.registry import load_metrics_registry
    from game_catalog_builder.pipelines.enrich_pipeline import process_steam_and_steamspy_streaming
    from game_catalog_builder.utils.utilities import write_csv

    input_csv = tmp_path / "Games_User.csv"
    steam_out = tmp_path / "Provider_Steam.csv"
    steamspy_out = tmp_path / "Provider_SteamSpy.csv"
    steam_cache = tmp_path / "steam_cache.json"
    steamspy_cache = tmp_path / "steamspy_cache.json"

    write_csv(pd.DataFrame([{"RowId": "rid:1", "Name": "Example Game"}]), input_csv)
    write_csv(
        pd.DataFrame(
            [
                {
                    "RowId": "rid:1",
                    "Name": "Example Game",
                    "Steam_AppID": "123",
                    "Steam_Name": "Example Game",
                }
            ]
        ),
        steam_out,
    )

    def fake_get(_self, url, params=None, timeout=None):
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                if "steamspy.com" in url:
                    return {
                        "owners": "1 .. 2",
                        "players_forever": 1,
                        "ccu": 1,
                        "average_forever": 1,
                    }
                # Should not be called in this test (Steam is already processed).
                raise AssertionError(f"unexpected url {url}")

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    (tmp_path / "metrics.yaml").write_text(
        "\n".join(
            [
                "version: 2",
                "metrics:",
                "  steamspy.owners: { column: SteamSpy_Owners, type: string }",
                "  steamspy.players: { column: SteamSpy_Players, type: string }",
                "  steamspy.players_2weeks: { column: SteamSpy_Players2Weeks, type: string }",
                "  steamspy.ccu: { column: SteamSpy_CCU, type: string }",
                "  steamspy.playtime_avg: { column: SteamSpy_PlaytimeAvg, type: string }",
                "  steamspy.playtime_avg_2weeks: { column: SteamSpy_PlaytimeAvg2Weeks, type: string }",
                "  steamspy.playtime_median_2weeks: { column: SteamSpy_PlaytimeMedian2Weeks, type: string }",
                "  steamspy.positive: { column: SteamSpy_Positive, type: string }",
                "  steamspy.negative: { column: SteamSpy_Negative, type: string }",
                "  steamspy.popularity.tags: { column: SteamSpy_Tags, type: json }",
                "  steamspy.popularity.tags_top: { column: SteamSpy_TagsTop, type: string }",
                "  steamspy.score_100: { column: SteamSpy_Score_100, type: string }",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    registry = load_metrics_registry(tmp_path / "metrics.yaml")

    process_steam_and_steamspy_streaming(
        input_csv=input_csv,
        steam_output_csv=steam_out,
        steamspy_output_csv=steamspy_out,
        steam_cache_path=steam_cache,
        steamspy_cache_path=steamspy_cache,
        registry=registry,
        identity_overrides=None,
    )

    out = pd.read_csv(steamspy_out, dtype=str, keep_default_na=False)
    assert out.loc[0, "SteamSpy_Owners"] == "1 .. 2"
