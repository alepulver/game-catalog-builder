from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


def test_normalize_generates_rowids_and_is_stable(tmp_path: Path) -> None:
    from game_catalog_builder.cli import _normalize_catalog

    inp = tmp_path / "user.csv"
    out = tmp_path / "catalog.csv"

    pd.DataFrame([{"Name": "A"}, {"Name": "B"}]).to_csv(inp, index=False)
    _normalize_catalog(inp, out)

    first = pd.read_csv(out, dtype=str, keep_default_na=False)
    assert "RowId" in first.columns
    assert first["RowId"].str.strip().ne("").all()
    first_ids = first["RowId"].tolist()

    # Running normalize on the already-normalized file should not change existing RowIds.
    _normalize_catalog(out, out)
    second = pd.read_csv(out, dtype=str, keep_default_na=False)
    assert second["RowId"].tolist() == first_ids


def test_normalize_rejects_duplicate_rowids(tmp_path: Path) -> None:
    from game_catalog_builder.cli import _normalize_catalog

    inp = tmp_path / "user.csv"
    out = tmp_path / "catalog.csv"

    pd.DataFrame([{"RowId": "rid:1", "Name": "A"}, {"RowId": "rid:1", "Name": "B"}]).to_csv(
        inp, index=False
    )
    with pytest.raises(SystemExit):
        _normalize_catalog(inp, out)
