from __future__ import annotations

import logging
from pathlib import Path

from ..metrics.registry import default_metrics_registry_path, load_metrics_registry
from ..pipelines.artifacts import ArtifactStore
from ..pipelines.helpers import build_catalog_store
from ..pipelines.common import log_cache_stats, write_full_csv
from ..pipelines.context import PipelineContext
from ..utils import read_csv
from .diagnostics.resolve import ResolveStats, resolve_catalog_pins


def run_resolve(
    ctx: PipelineContext,
    *,
    catalog_csv: Path,
    out_csv: Path,
    retry_missing: bool,
    apply: bool,
    use_jsonl: bool = True,
) -> ResolveStats:
    ctx.cache_dir.mkdir(parents=True, exist_ok=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    registry_path = default_metrics_registry_path()
    registry = load_metrics_registry(registry_path)

    tags_col, _t = registry.diagnostic_column_for_key("diagnostics.review.tags") or ("ReviewTags", "string")
    conf_col, _c = registry.diagnostic_column_for_key("diagnostics.match.confidence") or (
        "MatchConfidence",
        "string",
    )

    if not catalog_csv.exists():
        raise SystemExit(f"Catalog file not found: {catalog_csv}")

    catalog_jsonl = catalog_csv.parent / "jsonl" / f"{catalog_csv.stem}.jsonl"
    store = build_catalog_store(catalog_csv=catalog_csv, registry=registry, use_jsonl=use_jsonl)

    if use_jsonl:
        if not catalog_jsonl.exists():
            raise SystemExit(
                f"Missing internal catalog JSONL: {catalog_jsonl}. Run `import --diagnostics` first."
            )
        df = store.load_catalog(catalog_csv)
        if tags_col not in df.columns or conf_col not in df.columns:
            raise SystemExit(f"{catalog_jsonl} is missing diagnostics; run `import --diagnostics` first.")
    else:
        df = read_csv(catalog_csv)
        if tags_col not in df.columns or conf_col not in df.columns:
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
        registry=registry,
    )

    if apply:
        write_full_csv(df, out_csv)
        if use_jsonl:
            # Update internal JSONL source-of-truth alongside the catalog CSV.
            store.write_catalog(df, catalog_csv)
    else:
        logging.info("Resolve dry-run: no catalog CSV was written (use --apply to persist changes).")

    log_cache_stats(clients)

    return stats
