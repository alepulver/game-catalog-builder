from __future__ import annotations

from pathlib import Path

from ..metrics.registry import MetricsRegistry, default_metrics_registry_path, load_metrics_registry
from .artifacts import ArtifactStore


def resolve_metrics_registry(run_dir: Path | None, override: Path | None = None) -> MetricsRegistry:
    """
    Resolve the metrics registry path with a single rule:
    - use override when provided
    - else use default_metrics_registry_path(run_dir=run_dir)
    """
    path = override or default_metrics_registry_path(run_dir=run_dir)
    return load_metrics_registry(path)


def build_catalog_store(*, catalog_csv: Path, registry: MetricsRegistry, use_jsonl: bool) -> ArtifactStore:
    """
    Build an ArtifactStore configured for a catalog file.
    """
    return ArtifactStore(
        run_dir=catalog_csv.parent,
        registry=registry,
        use_jsonl=use_jsonl,
        reuse_jsonl=False,
        jsonl_dir=catalog_csv.parent / "jsonl",
    )
