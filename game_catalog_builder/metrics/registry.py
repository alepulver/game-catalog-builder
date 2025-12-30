from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


def default_metrics_registry_path(*, run_dir: Path | None = None) -> Path:
    """
    Default metrics registry path used by most commands.

    Prefers the user override `<run_dir>/metrics-registry.yaml` when present, otherwise falls back
    to `<run_dir>/metrics-registry.example.yaml`.

    When `run_dir` is not provided, uses the default run directory `data/`.
    """
    base = Path(run_dir) if run_dir is not None else Path("data")
    p = base / "metrics-registry.yaml"
    if p.exists():
        return p
    return base / "metrics-registry.example.yaml"


@dataclass(frozen=True)
class MetricsRegistry:
    """
    Metrics registry (v2):
    - canonical metric key -> (csv column name, value type)
    - plus a reverse map (csv column -> metric key)

    This intentionally avoids "fallback" keys: every metric column we want in JSONL must be
    explicitly registered.
    """

    by_key: dict[str, tuple[str, str]]
    by_column: dict[str, tuple[str, str]]
    diagnostics_by_key: dict[str, tuple[str, str]]
    diagnostics_by_column: dict[str, tuple[str, str]]

    def key_for_column(self, col: str) -> tuple[str, str] | None:
        """
        Return (metric_key, value_type) or None if the column is not registered.
        """
        return self.by_column.get(col)

    def column_for_key(self, key: str) -> tuple[str, str] | None:
        """
        Return (csv_column, value_type) or None if the metric key is not registered.
        """
        return self.by_key.get(key)

    def diagnostic_key_for_column(self, col: str) -> tuple[str, str] | None:
        """
        Return (diagnostic_key, value_type) or None if the column is not a registered diagnostic.
        """
        return self.diagnostics_by_column.get(col)

    def diagnostic_column_for_key(self, key: str) -> tuple[str, str] | None:
        """
        Return (csv_column, value_type) or None if the diagnostic key is not registered.
        """
        return self.diagnostics_by_key.get(key)

    @property
    def metric_columns(self) -> set[str]:
        return set(self.by_column.keys())

    @property
    def diagnostic_columns(self) -> set[str]:
        return set(self.diagnostics_by_column.keys())


def _as_str(x: Any) -> str:
    return str(x or "").strip()


def _default_column_for_key(key: str) -> str:
    """
    Deterministic column name generator for v2 registries.

    This is meant to reduce manual churn when adding new metrics keys. The recommended practice
    is still to explicitly set `column:` for existing stable CSVs.
    """
    k = str(key or "").strip()
    if not k:
        return ""
    parts = k.split(".")
    if not parts:
        return ""
    head = parts[0].casefold()
    rest = "_".join(p for p in parts[1:] if p).strip()
    if not rest:
        rest = "value"

    prefix = {
        "rawg": "RAWG_",
        "igdb": "IGDB_",
        "steam": "Steam_",
        "steamspy": "SteamSpy_",
        "hltb": "HLTB_",
        "wikidata": "Wikidata_",
    }.get(head)
    if prefix:
        return f"{prefix}{rest}"
    return k.replace(".", "_")


def load_metrics_registry(path: str | Path) -> MetricsRegistry:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError("metrics registry must be a YAML mapping")

    version = int(data.get("version") or 0)
    if version != 2:
        raise ValueError(f"Unsupported metrics registry version={version} in {p}. Expected version: 2")

    metrics_in = data.get("metrics") or {}
    if not isinstance(metrics_in, dict):
        raise ValueError("metrics registry version=2 requires a 'metrics' mapping")

    diagnostics_in = data.get("diagnostics") or {}
    if diagnostics_in is None:
        diagnostics_in = {}
    if not isinstance(diagnostics_in, dict):
        raise ValueError("metrics registry version=2 requires 'diagnostics' to be a mapping")

    by_key: dict[str, tuple[str, str]] = {}
    by_column: dict[str, tuple[str, str]] = {}
    diag_by_key: dict[str, tuple[str, str]] = {}
    diag_by_column: dict[str, tuple[str, str]] = {}

    for key_raw, spec in metrics_in.items():
        key = _as_str(key_raw)
        if not key:
            continue
        if not isinstance(spec, dict):
            raise ValueError(f"metrics['{key}'] must be a mapping")
        col = _as_str(spec.get("column")) or _default_column_for_key(key)
        typ = _as_str(spec.get("type")) or "string"
        if not col:
            raise ValueError(f"metrics['{key}'] missing required 'column' (and could not infer)")
        if key in by_key:
            raise ValueError(f"Duplicate metric key: {key}")
        if col in by_column:
            raise ValueError(f"CSV column mapped twice: {col}")
        by_key[key] = (col, typ)
        by_column[col] = (key, typ)

    for key_raw, spec in diagnostics_in.items():
        key = _as_str(key_raw)
        if not key:
            continue
        if not isinstance(spec, dict):
            raise ValueError(f"diagnostics['{key}'] must be a mapping")
        col = _as_str(spec.get("column")) or _default_column_for_key(key)
        typ = _as_str(spec.get("type")) or "string"
        if not col:
            raise ValueError(f"diagnostics['{key}'] missing required 'column' (and could not infer)")
        if key in diag_by_key:
            raise ValueError(f"Duplicate diagnostic key: {key}")
        if col in diag_by_column:
            raise ValueError(f"CSV diagnostic column mapped twice: {col}")
        if col in by_column:
            raise ValueError(f"CSV column mapped as both metric and diagnostic: {col}")
        diag_by_key[key] = (col, typ)
        diag_by_column[col] = (key, typ)

    return MetricsRegistry(
        by_key=by_key,
        by_column=by_column,
        diagnostics_by_key=diag_by_key,
        diagnostics_by_column=diag_by_column,
    )
