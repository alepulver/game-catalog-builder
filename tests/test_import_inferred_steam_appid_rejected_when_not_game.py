from __future__ import annotations

import argparse

import pandas as pd


def test_import_rejects_inferred_steam_appid_when_appdetails_not_game(tmp_path, monkeypatch):
    from game_catalog_builder import cli as cli_mod

    class FakeIGDBClient:
        def __init__(self, **_kwargs):
            pass

        def get_by_id(self, _igdb_id: str):
            return {
                "IGDB_ID": "123",
                "IGDB_Name": "Car Mechanic Simulator 2021",
                "IGDB_SteamAppID": "2112230",
                "IGDB_Year": "2021",
            }

        def search(self, *_args, **_kwargs):
            raise AssertionError("unexpected IGDB search in this test")

        def format_cache_stats(self) -> str:
            return "ok"

    class FakeSteamClient:
        def __init__(self, **_kwargs):
            pass

        def get_app_details(self, appid: int):
            if appid == 2112230:
                return {"name": "Car Mechanic Simulator 2021 - Aston Martin DLC", "type": "dlc"}
            if appid == 1234:
                return {"name": "Car Mechanic Simulator 2021", "type": "game"}
            raise AssertionError(f"unexpected appid {appid}")

        def search_appid(self, *_args, **_kwargs):
            return {"id": 1234, "name": "Car Mechanic Simulator 2021"}

        def format_cache_stats(self) -> str:
            return "ok"

    monkeypatch.setattr(cli_mod, "IGDBClient", FakeIGDBClient)
    monkeypatch.setattr(cli_mod, "SteamClient", FakeSteamClient)
    monkeypatch.setattr(
        cli_mod,
        "load_credentials",
        lambda _p: {"igdb": {"client_id": "x", "client_secret": "y"}},
    )

    input_csv = tmp_path / "Games_User.csv"
    output_csv = tmp_path / "Games_Catalog.csv"
    log_file = tmp_path / "log.txt"

    pd.DataFrame([{"Name": "Car Mechanic Simulator 2021", "IGDB_ID": "123"}]).to_csv(
        input_csv, index=False
    )

    args = argparse.Namespace(
        input=input_csv,
        out=output_csv,
        log_file=log_file,
        cache=tmp_path / "cache",
        credentials=tmp_path / "credentials.yaml",
        source="igdb,steam",
        diagnostics=True,
        debug=False,
        command="import",
    )

    cli_mod._command_normalize(args)

    out = pd.read_csv(output_csv, dtype=str).fillna("")
    # Should fall back to name search result, not keep inferred DLC appid.
    assert out.loc[0, "Steam_AppID"] == "1234"
