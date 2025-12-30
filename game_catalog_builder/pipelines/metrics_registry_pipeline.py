from __future__ import annotations

import logging
from pathlib import Path

from ..metrics.registry import load_metrics_registry
from ..schema import PINNED_ID_COLS, PROVIDER_PREFIXES
from ..utils.utilities import read_csv


def _is_metric_candidate(col: str) -> bool:
    c = str(col or "").strip()
    if not c:
        return False
    if c in PINNED_ID_COLS:
        return False
    if c.startswith(PROVIDER_PREFIXES):
        return True
    return False


def check_metrics_registry(*, registry_yaml: Path, csv_path: Path) -> None:
    """
    Validate a metrics registry (v2) against a concrete CSV header.

    This is informational: unmapped metric columns are allowed (selection feature).
    """
    reg = load_metrics_registry(registry_yaml)
    df = read_csv(csv_path)
    cols = list(df.columns)

    mapped_cols = reg.metric_columns
    metric_candidates = {c for c in cols if _is_metric_candidate(c)}

    missing = sorted(metric_candidates - mapped_cols)
    extra = sorted(mapped_cols - set(cols))

    logging.info(
        "Metrics registry check: mapped=%d csv_cols=%d metric_candidates=%d",
        len(mapped_cols),
        len(cols),
        len(metric_candidates),
    )

    if missing:
        sample = ", ".join(missing[:30])
        logging.info(
            "ℹ Unmapped metric columns in CSV (omitted from JSONL/export): %d (%s%s)",
            len(missing),
            sample,
            " ..." if len(missing) > 30 else "",
        )
    else:
        logging.info("✔ All metric candidate columns are mapped by the registry")

    if extra:
        extra_keys: list[str] = []
        for c in extra:
            k = reg.key_for_column(c)
            if k is None:
                continue
            extra_keys.append(k[0])
        extra_keys = sorted(set(extra_keys))
        sample = ", ".join(extra_keys[:30])
        logging.info(
            "ℹ Unused registry metrics for this CSV header (not present as columns): %d (%s%s)",
            len(extra_keys),
            sample,
            " ..." if len(extra_keys) > 30 else "",
        )
