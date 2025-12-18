from __future__ import annotations

import argparse

import pandas as pd


def test_import_inferred_steam_appid_does_not_overwrite_with_name_search(tmp_path, monkeypatch):
    from game_catalog_builder import cli as cli_mod

    class FakeIGDBClient:
        def __init__(self, **_kwargs):
            pass

        def get_by_id(self, _igdb_id: str):
            return {
                "IGDB_ID": "123",
                "IGDB_Name": "Half-Life 2: Episode One",
                "IGDB_SteamAppID": "380",
                "IGDB_Year": "2006",
            }

        def search(self, *_args, **_kwargs):
            raise AssertionError("unexpected IGDB search in this test")

        def format_cache_stats(self) -> str:
            return "by_query hit=0 fetch=0 (neg hit=0 fetch=0), by_id hit=0 fetch=0"

    class FakeSteamClient:
        def __init__(self, **_kwargs):
            pass

        def get_app_details(self, appid: int):
            assert appid == 380
            return {"name": "Half-Life 2: Episode One", "type": "game"}

        def search_appid(self, *_args, **_kwargs):
            raise AssertionError("name-based Steam search should be skipped when inferred ID exists")

        def format_cache_stats(self) -> str:
            return "by_query hit=0 fetch=0 (neg hit=0 fetch=0), by_id hit=0 fetch=0"

    monkeypatch.setattr(cli_mod, "IGDBClient", FakeIGDBClient)
    monkeypatch.setattr(cli_mod, "SteamClient", FakeSteamClient)
    monkeypatch.setattr(cli_mod, "load_credentials", lambda _p: {"igdb": {"client_id": "x", "client_secret": "y"}})

    input_csv = tmp_path / "Games_User.csv"
    output_csv = tmp_path / "Games_Catalog.csv"
    log_file = tmp_path / "log.txt"

    pd.DataFrame([{"Name": "Half-Life 2: Episode One", "IGDB_ID": "123"}]).to_csv(
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
    assert out.loc[0, "Steam_AppID"] == "380"
    assert out.loc[0, "Steam_MatchedName"] == "Half-Life 2: Episode One"
