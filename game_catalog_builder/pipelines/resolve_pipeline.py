from __future__ import annotations

import logging
from pathlib import Path

from ..analysis.resolve import ResolveStats, resolve_catalog_pins
from ..pipelines.common import log_cache_stats, write_full_csv
from ..pipelines.context import PipelineContext
from ..utils import read_csv


def run_resolve(
    ctx: PipelineContext,
    *,
    catalog_csv: Path,
    out_csv: Path,
    retry_missing: bool,
    apply: bool,
) -> ResolveStats:
    ctx.cache_dir.mkdir(parents=True, exist_ok=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    if not catalog_csv.exists():
        raise SystemExit(f"Catalog file not found: {catalog_csv}")

    df = read_csv(catalog_csv)
    if "ReviewTags" not in df.columns or "MatchConfidence" not in df.columns:
        raise SystemExit(
            f"{catalog_csv} is missing diagnostics columns; run `import --diagnostics` first."
        )

    clients = ctx.build_clients()

    df, stats = resolve_catalog_pins(
        df,
        sources=set(ctx.sources),
        clients=clients,
        retry_missing=retry_missing,
        apply=apply,
    )

    if apply:
        write_full_csv(df, out_csv)
    else:
        logging.info(
            "Resolve dry-run: no catalog CSV was written (use --apply to persist changes)."
        )

    log_cache_stats(clients)

    return stats
