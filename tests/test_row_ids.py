from __future__ import annotations

from pathlib import Path

import pandas as pd


def test_ensure_row_ids_in_input_adds_and_persists(tmp_path: Path):
    from game_catalog_builder.utils.utilities import (
        ensure_row_ids,
        load_identity_overrides,
        write_csv,
    )

    p = tmp_path / "input.csv"
    write_csv(pd.DataFrame([{"Name": "Doom"}, {"Name": "Doom"}]), p)

    df0 = pd.read_csv(p, dtype=str, keep_default_na=False)
    df1, created = ensure_row_ids(df0)
    assert "RowId" in df1.columns
    assert df1["RowId"].astype(str).str.strip().ne("").all()
    assert df1["RowId"].nunique() == 2
    assert created == 2

    df2, created2 = ensure_row_ids(df1)
    assert df2["RowId"].tolist() == df1["RowId"].tolist()
    assert created2 == 0

    # Basic smoke: identity overrides loader reads IDs directly.
    identity = tmp_path / "Games_Identity.csv"
    write_csv(pd.DataFrame([{"RowId": df1.iloc[0]["RowId"], "RAWG_ID": "123"}]), identity)
    overrides = load_identity_overrides(identity)
    assert overrides[df1.iloc[0]["RowId"]]["RAWG_ID"] == "123"


def test_ensure_row_ids_in_input_fixes_duplicates(tmp_path: Path):
    from game_catalog_builder.utils.utilities import ensure_row_ids_in_input, write_csv

    p = tmp_path / "input.csv"
    write_csv(
        pd.DataFrame([{"RowId": "rid:dup", "Name": "A"}, {"RowId": "rid:dup", "Name": "B"}]), p
    )

    df = ensure_row_ids_in_input(p)
    assert df["RowId"].nunique() == 2
