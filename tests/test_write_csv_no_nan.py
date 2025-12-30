from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from game_catalog_builder.utils.utilities import write_csv


def test_write_csv_does_not_emit_nan_tokens(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {"RowId": "rid:1", "Name": "Doom", "A": float("nan"), "B": pd.NA},
            {"RowId": "rid:2", "Name": "Quake", "A": None, "B": ""},
        ]
    )
    out = tmp_path / "out.csv"
    write_csv(df, out)

    with out.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    for row in rows:
        for cell in row:
            assert cell.strip().casefold() != "nan"
