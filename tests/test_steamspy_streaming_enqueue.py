from __future__ import annotations

from pathlib import Path

import pandas as pd


def test_steamspy_streaming_enqueues_existing_appids(tmp_path: Path, monkeypatch):
    """
    Regression test: if Steam_AppID is already present in Provider_Steam.csv, the streaming pipeline
    must still enqueue those rows so SteamSpy can populate Provider_SteamSpy.csv.
    """
    from game_catalog_builder.cli import process_steam_and_steamspy_streaming
    from game_catalog_builder.utils.utilities import write_csv

    input_csv = tmp_path / "Games_User.csv"
    steam_out = tmp_path / "Provider_Steam.csv"
    steamspy_out = tmp_path / "Provider_SteamSpy.csv"
    steam_cache = tmp_path / "steam_cache.json"
    steamspy_cache = tmp_path / "steamspy_cache.json"

    write_csv(pd.DataFrame([{"RowId": "rid:1", "Name": "Example Game"}]), input_csv)
    write_csv(
        pd.DataFrame([{"RowId": "rid:1", "Name": "Example Game", "Steam_AppID": "123"}]), steam_out
    )

    def fake_get(url, params=None, timeout=None):
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

    monkeypatch.setattr("requests.get", fake_get)

    process_steam_and_steamspy_streaming(
        input_csv=input_csv,
        steam_output_csv=steam_out,
        steamspy_output_csv=steamspy_out,
        steam_cache_path=steam_cache,
        steamspy_cache_path=steamspy_cache,
        identity_overrides=None,
    )

    out = pd.read_csv(steamspy_out, dtype=str, keep_default_na=False)
    assert out.loc[0, "SteamSpy_Owners"] == "1 .. 2"
