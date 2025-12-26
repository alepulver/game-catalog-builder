from __future__ import annotations

import argparse

import pandas as pd


def test_import_ignores_pinned_steam_appid_when_appdetails_missing_and_researches(
    tmp_path, monkeypatch
):
    from game_catalog_builder import cli as cli_mod
    from game_catalog_builder.pipelines import context, provider_clients

    class FakeSteamClient:
        def __init__(self, **_kwargs):
            pass

        def get_app_details(self, appid: int):
            # Simulate a Steam "sub/package id" that can't be resolved by appdetails.
            if appid == 77828:
                return None
            if appid == 335670:
                return {"name": "LISA: The Painful", "type": "game"}
            raise AssertionError(f"unexpected appid {appid}")

        def search_appid(self, *_args, **_kwargs):
            return {"id": 335670, "name": "LISA: The Painful", "type": "app"}

        def format_cache_stats(self) -> str:
            return "ok"

    monkeypatch.setattr(provider_clients, "SteamClient", FakeSteamClient)
    monkeypatch.setattr(context, "load_credentials", lambda _p: {})

    input_csv = tmp_path / "Games_Catalog.csv"
    output_csv = tmp_path / "Games_Catalog.csv"
    log_file = tmp_path / "log.txt"

    pd.DataFrame([{"Name": "LISA: Complete Edition", "Steam_AppID": "77828"}]).to_csv(
        input_csv, index=False
    )

    args = argparse.Namespace(
        input=input_csv,
        out=output_csv,
        log_file=log_file,
        cache=tmp_path / "cache",
        credentials=tmp_path / "credentials.yaml",
        source="steam",
        diagnostics=True,
        debug=False,
        command="import",
    )

    cli_mod._command_normalize(args)

    out = pd.read_csv(output_csv, dtype=str).fillna("")
    assert out.loc[0, "Steam_AppID"] == "335670"
