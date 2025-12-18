from __future__ import annotations


def test_import_hltb_populates_id_even_without_diagnostics(tmp_path, monkeypatch):
    import pandas as pd

    from game_catalog_builder.cli import main

    # Minimal catalog-like CSV (no RowId; import will generate it).
    input_csv = tmp_path / "user.csv"
    pd.DataFrame([{"Name": "Spider-Man 2 (2004)", "YearHint": "2004"}]).to_csv(
        input_csv, index=False
    )

    out_csv = tmp_path / "catalog.csv"

    def fake_search(self, game_name, *, query=None, hltb_id=None):
        assert game_name == "Spider-Man 2 (2004)"
        return {
            "HLTB_ID": "8940",
            "HLTB_Name": "Spider-Man 2",
            "HLTB_Main": "1",
            "HLTB_Extra": "",
            "HLTB_Completionist": "",
        }

    monkeypatch.setattr("game_catalog_builder.clients.hltb_client.HLTBClient.search", fake_search)

    # Also stub credentials loading to avoid requiring real credentials for other providers.
    monkeypatch.setattr(
        "game_catalog_builder.cli.load_credentials",
        lambda *_args, **_kwargs: {"igdb": {}, "rawg": {}},
    )

    main(
        [
            "import",
            str(input_csv),
            "--out",
            str(out_csv),
            "--source",
            "hltb",
            "--no-diagnostics",
            "--cache",
            str(tmp_path / "cache"),
        ]
    )

    df = pd.read_csv(out_csv, dtype=str, keep_default_na=False)
    assert df.loc[0, "HLTB_ID"] == "8940"
