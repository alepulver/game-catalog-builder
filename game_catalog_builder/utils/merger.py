from __future__ import annotations

from pathlib import Path
from typing import cast

import pandas as pd

from ..metrics.registry import MetricsRegistry, default_metrics_registry_path, load_metrics_registry
from ..schema import PINNED_ID_COLS


def reorder_columns(
    df: pd.DataFrame,
    *,
    registry: MetricsRegistry | None = None,
    metrics_registry_path: str | Path | None = None,
) -> pd.DataFrame:
    """
    Registry-driven column order.

    Logical order:
      - Personal (user) columns + pin columns (stable up front)
      - Derived + composite metrics
      - Provider metrics (grouped by provider)
      - Diagnostics (review/matching columns)

    Notes:
    - CSV columns are a presentation layer; internal canonical identities are dotted metric keys.
    - The metrics registry is the canonical source of "what is a metric" vs "user-owned field".
    """
    reg = registry
    if reg is None:
        path = Path(metrics_registry_path) if metrics_registry_path else default_metrics_registry_path()
        reg = load_metrics_registry(path)

    metric_cols = reg.metric_columns
    diag_cols = reg.diagnostic_columns

    def _is_personal(col: str) -> bool:
        if col.startswith("__"):
            return False
        if col in diag_cols:
            return False
        if col in metric_cols:
            return False
        return True

    base_front = ["RowId", "Name", "Disabled", "YearHint", "Platform"]
    pinned_front = [
        c
        for c in ("RAWG_ID", "IGDB_ID", "Steam_AppID", "HLTB_ID", "HLTB_Query", "Wikidata_QID")
        if c in df.columns
    ]

    personal_cols = [c for c in df.columns if _is_personal(c) and c not in PINNED_ID_COLS]

    derived_cols: list[str] = []
    for key, (col, _typ) in reg.by_key.items():
        if key.startswith(("derived.", "composite.")) and col in df.columns:
            derived_cols.append(col)

    provider_heads = ["rawg", "igdb", "steam", "steamspy", "hltb", "wikidata", "wikipedia"]
    provider_cols_by_head: dict[str, list[str]] = {h: [] for h in provider_heads}
    for key, (col, _typ) in reg.by_key.items():
        head = str(key.split(".", 1)[0] if "." in key else key).casefold()
        if head in provider_cols_by_head and col in df.columns and col not in PINNED_ID_COLS:
            if key.startswith(("derived.", "composite.")):
                continue
            provider_cols_by_head[head].append(col)

    diagnostics_cols = [c for c in sorted(diag_cols) if c in df.columns]

    ordered: list[str] = []
    for c in base_front:
        if c in df.columns and c not in ordered:
            ordered.append(c)
    for c in pinned_front:
        if c not in ordered:
            ordered.append(c)
    for c in personal_cols:
        if c not in ordered:
            ordered.append(c)

    for c in sorted(set(derived_cols)):
        if c not in ordered:
            ordered.append(c)

    for head in provider_heads:
        for c in sorted(set(provider_cols_by_head.get(head, []))):
            if c not in ordered:
                ordered.append(c)

    for c in diagnostics_cols:
        if c not in ordered:
            ordered.append(c)

    remaining = [c for c in df.columns if c not in ordered]
    return cast(pd.DataFrame, df[ordered + remaining])
