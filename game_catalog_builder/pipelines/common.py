from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Iterator, cast

import pandas as pd

from ..config import CLI
from ..schema import provider_output_columns
from ..utils.progress import Progress


def total_named_rows(df: pd.DataFrame, *, col: str = "Name") -> int:
    if col not in df.columns:
        return 0
    return int((df[col].astype(str).str.strip() != "").sum())


def log_cache_stats(clients: dict[str, object]) -> None:
    order: list[tuple[str, str]] = [
        ("rawg", "[RAWG]"),
        ("igdb", "[IGDB]"),
        ("steam", "[STEAM]"),
        ("steamspy", "[STEAMSPY]"),
        ("hltb", "[HLTB]"),
        ("wikidata", "[WIKIDATA]"),
        ("wikipedia_pageviews", "[WIKIPEDIA]"),
        ("wikipedia_summary", "[WIKIPEDIA] Summary"),
    ]

    for prov, label in order:
        client = clients.get(prov)
        if client is None:
            continue
        fmt = getattr(client, "format_cache_stats", None)
        if not callable(fmt):
            continue
        try:
            logging.info(f"{label} Cache stats: {fmt()}")
        except Exception:
            # Avoid failing pipelines because of a stats formatting bug.
            logging.info(f"{label} Cache stats: (unavailable)")


def iter_named_rows_with_progress(
    df: pd.DataFrame,
    *,
    label: str,
    total: int | None,
    skip_row: Callable[[pd.Series], bool] | None = None,
) -> Iterator[tuple[int, pd.Series, str, int]]:
    """
    Iterate rows with a non-empty Name while emitting periodic progress logs.

    Yields: (idx, row, name, seen_index)
    """
    progress = Progress(label, total=total or None, every_n=CLI.progress_every_n)
    seen = 0
    for idx, row in df.iterrows():
        try:
            pos = df.index.get_loc(idx)
        except Exception:
            continue
        if not isinstance(pos, int):
            continue
        if skip_row is not None and skip_row(row):
            continue
        name = str(row.get("Name", "") or "").strip()
        if not name:
            continue
        seen += 1
        progress.maybe_log(seen)
        yield pos, row, name, seen


def flush_pending_keys(
    pending: dict[object, list[int]],
    *,
    fetch_many: Callable[[list[object]], dict[Any, Any]],
    on_item: Callable[[object, list[int], Any], int],
) -> int:
    """
    Flush a pending key->indices buffer using a batched fetch function.

    - `fetch_many(keys)` returns a mapping keyed by the same key type (or a compatible lookup).
    - `on_item(key, indices, fetched_value)` applies the fetched value to the dataframe.

    Returns number of row indices processed (len of all index lists flushed).
    """
    if not pending:
        return 0
    keys: list[object] = list(pending.keys())
    by_key = fetch_many(keys)
    processed = 0
    for key, indices in list(pending.items()):
        value = None
        try:
            if isinstance(by_key, dict):
                value = by_key.get(key)
        except Exception:
            value = None
        processed += int(on_item(key, indices, value) or 0)
    pending.clear()
    return processed


def write_provider_output_csv(
    df: pd.DataFrame,
    output_csv: Path,
    *,
    prefix: str,
    extra: tuple[str, ...] = (),
) -> None:
    """
    Write a provider-specific output CSV with base columns + provider-prefixed columns.
    """
    from ..utils import write_csv

    cols = provider_output_columns(list(df.columns), prefix=prefix, extra=extra)
    write_csv(cast(pd.DataFrame, df[cols]), output_csv)


def write_full_csv(df: pd.DataFrame, output_csv: Path) -> None:
    from ..utils import write_csv

    write_csv(df, output_csv)


def filter_rows_by_ids(df: pd.DataFrame, row_ids: set[str]) -> pd.DataFrame:
    """
    Return a dataframe containing only rows whose RowId is in row_ids.
    """
    if "RowId" not in df.columns:
        return df.iloc[0:0].copy()
    normalized = {str(r or "").strip() for r in row_ids if str(r or "").strip()}
    mask = df["RowId"].astype(str).str.strip().isin(normalized)
    return df.loc[mask].copy()
