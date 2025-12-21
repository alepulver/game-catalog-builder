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

    # Basic smoke: override loader reads pinned IDs from any CSV with RowId.
    overrides_csv = tmp_path / "catalog.csv"
    write_csv(pd.DataFrame([{"RowId": df1.iloc[0]["RowId"], "RAWG_ID": "123"}]), overrides_csv)
    overrides = load_identity_overrides(overrides_csv)
    assert overrides[df1.iloc[0]["RowId"]]["RAWG_ID"] == "123"


def test_ensure_row_ids_in_input_fixes_duplicates(tmp_path: Path):
    from game_catalog_builder.utils.utilities import ensure_row_ids, write_csv

    p = tmp_path / "input.csv"
    write_csv(
        pd.DataFrame([{"RowId": "rid:dup", "Name": "A"}, {"RowId": "rid:dup", "Name": "B"}]), p
    )

    df0 = pd.read_csv(p, dtype=str, keep_default_na=False)
    df, _ = ensure_row_ids(df0)
    assert df["RowId"].nunique() == 2
