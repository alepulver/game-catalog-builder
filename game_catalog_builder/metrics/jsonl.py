from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from ..schema import PINNED_ID_COLS, PROVIDER_PREFIXES
from .registry import MetricsRegistry


def _manifest_path_for_jsonl(path: Path) -> Path:
    # Provider_rawg.jsonl -> Provider_rawg.manifest.json
    # Games_Enriched.jsonl -> Games_Enriched.manifest.json
    p = Path(path)
    if p.suffix != ".jsonl":
        return p.with_suffix(p.suffix + ".manifest.json")
    return p.with_suffix("").with_suffix(".manifest.json")


def write_jsonl_manifest(
    *,
    jsonl_path: Path,
    df_columns: list[str],
    column_to_metric_key: dict[str, str] | None = None,
) -> None:
    mp = _manifest_path_for_jsonl(jsonl_path)
    mp.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {"columns": list(df_columns)}
    if column_to_metric_key:
        payload["column_to_metric_key"] = dict(column_to_metric_key)
    mp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _is_metric_candidate(column: str, *, registry: MetricsRegistry) -> bool:
    """
    Determine whether an unmapped column should be treated as a "metric" (provider/derived)
    rather than as a user-owned personal column.

    Personal columns should always be preserved even if the registry doesn't mention them.
    Metric columns are selectable via the registry and should be omitted if not mapped.
    """
    c = str(column or "").strip()
    if not c:
        return False
    if c in registry.metric_columns:
        return True
    if c.startswith(PROVIDER_PREFIXES):
        return True
    return False


def _auto_metric_key_for_column(column: str) -> str:
    """
    Deterministic metric key for --all-metrics when a column is not present in the registry.

    This is only used when the user explicitly opts into --all-metrics. The canonical metric
    key is always dotted; CSV columns are just a view.

    Examples:
      RAWG_Added -> rawg.added
      Reach_Composite -> derived.reach_composite
    """

    def to_snake(s: str) -> str:
        s = str(s or "").strip().replace(" ", "_").replace("-", "_")
        if not s:
            return ""
        s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", s)
        s = re.sub(r"(?<=[A-Za-z])(?=[0-9])", "_", s)
        s = re.sub(r"(?<=[0-9])(?=[A-Za-z])", "_", s)
        s = re.sub(r"__+", "_", s)
        return s.strip("_").casefold()

    c = str(column or "").strip()
    if not c:
        return "derived.unknown"
    if c.startswith("RAWG_"):
        return f"rawg.{to_snake(c[len('RAWG_') :]) or 'value'}"
    if c.startswith("IGDB_"):
        return f"igdb.{to_snake(c[len('IGDB_') :]) or 'value'}"
    if c.startswith("SteamSpy_"):
        return f"steamspy.{to_snake(c[len('SteamSpy_') :]) or 'value'}"
    if c.startswith("Steam_"):
        return f"steam.{to_snake(c[len('Steam_') :]) or 'value'}"
    if c.startswith("HLTB_"):
        return f"hltb.{to_snake(c[len('HLTB_') :]) or 'value'}"
    if c.startswith("Wikidata_"):
        return f"wikidata.{to_snake(c[len('Wikidata_') :]) or 'value'}"
    return f"derived.{to_snake(c) or 'value'}"


def _manifest_columns(df_columns: Iterable[str], *, registry: MetricsRegistry) -> list[str]:
    """
    Preserve the DataFrame's column order, but drop metric candidate columns that are not mapped
    in the registry (selection feature).
    """
    out: list[str] = []
    for c in df_columns:
        col = str(c or "").strip()
        if not col:
            continue
        if _is_metric_candidate(col, registry=registry) and col not in registry.metric_columns:
            continue
        out.append(col)
    return out


def _coerce_value(value: Any, value_type: str) -> object | None:
    vt = (value_type or "string").strip().casefold()
    if vt == "string":
        if value is None:
            return None
        s = str(value).strip()
        return s if s else None
    if vt == "int":
        if isinstance(value, str) and value.strip():
            raise ValueError(f"Expected int metric value, got string: {value!r}")
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            return None
        return None
    if vt == "float":
        if isinstance(value, str) and value.strip():
            raise ValueError(f"Expected float metric value, got string: {value!r}")
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        return None
    if vt == "bool":
        if isinstance(value, str) and value.strip():
            raise ValueError(f"Expected bool metric value, got string: {value!r}")
        return value if isinstance(value, bool) else None
    if vt == "json":
        if isinstance(value, str) and value.strip():
            raise ValueError(f"Expected JSON metric value, got string: {value!r}")
        if isinstance(value, (list, dict)):
            return value
        return None
    if vt == "list_csv":
        if isinstance(value, str) and value.strip():
            raise ValueError(f"Expected list metric value, got string: {value!r}")
        if isinstance(value, list):
            return value
        return None
    # Unknown types: keep as string.
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _default_personal_columns(df_columns: Iterable[str], *, registry: MetricsRegistry) -> list[str]:
    cols = list(df_columns)
    out: list[str] = []
    for c in cols:
        if c == "RowId":
            continue
        if c in registry.diagnostic_columns:
            continue
        if c in PINNED_ID_COLS:
            continue
        if _is_metric_candidate(c, registry=registry):
            continue
        out.append(c)
    return out


def dataframe_to_rows(
    df: pd.DataFrame,
    *,
    registry: MetricsRegistry,
    include_diagnostics: bool,
    provider_name: str | None = None,
    include_all_metrics: bool = False,
) -> list[dict[str, object]]:
    """
    Convert a dataframe (catalog/provider/enriched) into JSON-serializable rows using the stable
    row envelope:

      {row_id, personal, pins, metrics, diagnostics, meta}
    """
    cols = list(df.columns)
    personal_cols = _default_personal_columns(cols, registry=registry)

    out_rows: list[dict[str, object]] = []
    omitted_metric_columns: set[str] = set()
    auto_metric_columns: set[str] = set()
    for _, r in df.iterrows():
        row = r.to_dict()
        row_id = str(row.get("RowId", "") or "").strip()
        if not row_id:
            continue

        personal: dict[str, object] = {}
        for c in personal_cols:
            v = str(row.get(c, "") or "").strip()
            if v:
                personal[c] = v

        pins: dict[str, object] = {}
        for c in sorted(PINNED_ID_COLS):
            if c not in row:
                continue
            v = str(row.get(c, "") or "").strip()
            if v:
                pins[c] = v

        metrics: dict[str, object] = {}
        for c in cols:
            if c == "RowId" or c in registry.diagnostic_columns or c in PINNED_ID_COLS:
                continue
            if c in personal_cols:
                continue
            v = row.get(c, "")
            if str(v or "").strip() == "":
                continue
            mapped = registry.key_for_column(c)
            if mapped is None:
                if _is_metric_candidate(c, registry=registry):
                    if not include_all_metrics:
                        omitted_metric_columns.add(c)
                        continue
                    auto_metric_columns.add(c)
                    key = _auto_metric_key_for_column(c)
                    vt = "string"
                else:
                    continue
            else:
                key, vt = mapped
            coerced = _coerce_value(v, vt)
            if coerced is None:
                continue
            metrics[key] = coerced

        diagnostics: dict[str, object] = {}
        if include_diagnostics:
            for c in sorted(registry.diagnostic_columns):
                if c not in row:
                    continue
                v = str(row.get(c, "") or "").strip()
                if v:
                    diagnostics[c] = v

        meta: dict[str, object] = {}
        if provider_name:
            meta["provider"] = provider_name

        out_rows.append(
            {
                "row_id": row_id,
                "personal": personal,
                "pins": pins,
                "metrics": metrics,
                "diagnostics": diagnostics,
                "meta": meta,
            }
        )
    if omitted_metric_columns:
        sample = ", ".join(sorted(omitted_metric_columns)[:20])
        logging.info(
            "ℹ Metrics registry omitted %d columns (not mapped): %s%s",
            len(omitted_metric_columns),
            sample,
            " ..." if len(omitted_metric_columns) > 20 else "",
        )
    if auto_metric_columns:
        sample = ", ".join(sorted(auto_metric_columns)[:20])
        logging.info(
            "ℹ Included %d unmapped metric columns via auto keys: %s%s",
            len(auto_metric_columns),
            sample,
            " ..." if len(auto_metric_columns) > 20 else "",
        )
    return out_rows


def write_jsonl(rows: Iterable[dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for obj in rows:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def write_json(rows: Iterable[dict[str, object]], path: Path) -> None:
    """
    Write a user-friendly JSON export (single file).

    This is not intended as the internal streamable format; JSONL is preferred for internal
    artifacts. This is for ad-hoc inspection and tooling that prefers a single JSON document.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    as_list = list(rows)
    path.write_text(json.dumps(as_list, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_dataframe_jsonl(
    df: pd.DataFrame,
    *,
    path: Path,
    registry: MetricsRegistry,
    include_diagnostics: bool,
    provider_name: str | None = None,
    include_all_metrics: bool = False,
) -> None:
    manifest_cols = (
        list(df.columns) if include_all_metrics else _manifest_columns(list(df.columns), registry=registry)
    )
    column_to_key: dict[str, str] = {}
    for c in manifest_cols:
        if c == "RowId" or c in registry.diagnostic_columns or c in PINNED_ID_COLS:
            continue
        if not _is_metric_candidate(c, registry=registry):
            continue
        mapped = registry.key_for_column(c)
        if mapped is None:
            if include_all_metrics:
                column_to_key[c] = _auto_metric_key_for_column(c)
            continue
        key, _typ = mapped
        column_to_key[c] = key
    rows = dataframe_to_rows(
        df,
        registry=registry,
        include_diagnostics=include_diagnostics,
        provider_name=provider_name,
        include_all_metrics=include_all_metrics,
    )
    write_jsonl(rows, path)
    write_jsonl_manifest(
        jsonl_path=path,
        df_columns=manifest_cols,
        column_to_metric_key=column_to_key,
    )
    logging.info(f"✔ JSONL written: {path}")


def write_dataframe_json(
    df: pd.DataFrame,
    *,
    path: Path,
    registry: MetricsRegistry,
    include_diagnostics: bool,
    provider_name: str | None = None,
) -> None:
    rows = dataframe_to_rows(
        df, registry=registry, include_diagnostics=include_diagnostics, provider_name=provider_name
    )
    write_json(rows, path)
    logging.info(f"✔ JSON written: {path}")


def load_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def load_jsonl_strict(path: Path) -> list[dict[str, object]]:
    """
    Strict JSONL loader for internal artifacts.

    Unlike `load_jsonl()`, this fails fast on invalid JSON lines so we don't silently "reuse"
    partial or corrupt outputs.
    """
    rows: list[dict[str, object]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception as e:
                raise ValueError(f"Invalid JSON in {path} line {i}: {e}") from e
            if not isinstance(obj, dict):
                raise ValueError(f"Invalid JSONL row type in {path} line {i}: expected object")
            rows.append(obj)
    return rows


def validate_jsonl_rows_against_registry(
    rows: Iterable[dict[str, object]],
    *,
    registry: MetricsRegistry,
    context: str,
) -> None:
    """
    Validate a set of JSONL rows (stable row envelope) against the metrics registry.

    This ensures `--reuse-jsonl` and JSONL-as-catalog are type-safe, and surfaces "old format"
    JSONLs early (e.g. numbers stored as strings).
    """

    def _expect_type(v: object, vt: str, *, key: str) -> None:
        if v is None:
            return
        t = (vt or "string").strip().casefold()
        if t == "string":
            if isinstance(v, str):
                return
            raise ValueError(f"{context}: metric {key} expected string, got {type(v).__name__}")
        if t == "int":
            if isinstance(v, bool):
                raise ValueError(f"{context}: metric {key} expected int, got bool")
            if isinstance(v, int):
                return
            if isinstance(v, float) and v.is_integer():
                return
            raise ValueError(f"{context}: metric {key} expected int, got {type(v).__name__}")
        if t == "float":
            if isinstance(v, bool):
                raise ValueError(f"{context}: metric {key} expected float, got bool")
            if isinstance(v, (int, float)):
                return
            raise ValueError(f"{context}: metric {key} expected float, got {type(v).__name__}")
        if t == "bool":
            if isinstance(v, bool):
                return
            raise ValueError(f"{context}: metric {key} expected bool, got {type(v).__name__}")
        if t == "json":
            if isinstance(v, (dict, list)):
                return
            raise ValueError(f"{context}: metric {key} expected json, got {type(v).__name__}")
        if t == "list_csv":
            if isinstance(v, list):
                return
            raise ValueError(f"{context}: metric {key} expected list, got {type(v).__name__}")
        # Unknown: don't validate.
        return

    for r in rows:
        if not isinstance(r, dict):
            raise ValueError(f"{context}: invalid JSONL row type: {type(r).__name__}")
        row_id = str(r.get("row_id", "") or "").strip()
        if not row_id:
            raise ValueError(f"{context}: missing row_id")
        metrics_raw = r.get("metrics")
        if metrics_raw is None:
            continue
        if not isinstance(metrics_raw, dict):
            raise ValueError(f"{context}: row {row_id} metrics must be an object")
        for k, v in metrics_raw.items():
            key = str(k or "").strip()
            if not key:
                continue
            mapped = registry.column_for_key(key)
            if mapped is None:
                continue
            _col, vt = mapped
            _expect_type(v, vt, key=key)


def index_jsonl_by_row_id(rows: Iterable[dict[str, object]]) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for r in rows:
        row_id = str(r.get("row_id", "") or "").strip()
        if not row_id:
            continue
        out[row_id] = r
    return out


def jsonl_rows_to_catalog_dataframe(
    rows: Iterable[dict[str, object]], *, include_diagnostics: bool = True
) -> pd.DataFrame:
    """
    Build a catalog-style DataFrame from internal JSONL rows (stable row envelope).

    This is intentionally a lossy *view*:
    - Flattens `personal`, `pins`, and optionally `diagnostics` into scalar columns.
    - Does not attempt to expand `metrics` into columns (that is the job of export via registry).
    """
    out_rows: list[dict[str, object]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        row_id = str(r.get("row_id", "") or "").strip()
        if not row_id:
            continue
        out: dict[str, object] = {"RowId": row_id}
        raw_personal = r.get("personal")
        raw_pins = r.get("pins")
        raw_diagnostics = r.get("diagnostics")
        personal: dict[str, object] = raw_personal if isinstance(raw_personal, dict) else {}
        pins: dict[str, object] = raw_pins if isinstance(raw_pins, dict) else {}
        diagnostics: dict[str, object] = raw_diagnostics if isinstance(raw_diagnostics, dict) else {}
        for k, v in personal.items():
            if str(k or "").strip():
                out[str(k)] = v
        for k, v in pins.items():
            if str(k or "").strip():
                out[str(k)] = v
        if include_diagnostics:
            for k, v in diagnostics.items():
                if str(k or "").strip():
                    out[str(k)] = v
        out_rows.append(out)
    return pd.DataFrame(out_rows)


def jsonl_rows_to_registered_dataframe(
    rows: Iterable[dict[str, object]],
    *,
    registry: MetricsRegistry,
    include_personal: bool = True,
    include_pins: bool = True,
    include_diagnostics: bool = False,
) -> pd.DataFrame:
    """
    Build a DataFrame view from JSONL rows using the metrics registry to map dotted metric keys
    back to their CSV column names.

    This is intended for in-memory reuse (skip provider calls) and intentionally does not do any
    CSV parsing or string-to-JSON coercion.
    """
    out_rows: list[dict[str, object]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        row_id = str(r.get("row_id", "") or "").strip()
        if not row_id:
            continue
        out: dict[str, object] = {"RowId": row_id}
        raw_personal = r.get("personal")
        raw_pins = r.get("pins")
        raw_metrics = r.get("metrics")
        raw_diagnostics = r.get("diagnostics")
        personal: dict[str, object] = raw_personal if isinstance(raw_personal, dict) else {}
        pins: dict[str, object] = raw_pins if isinstance(raw_pins, dict) else {}
        metrics: dict[str, object] = raw_metrics if isinstance(raw_metrics, dict) else {}
        diagnostics: dict[str, object] = raw_diagnostics if isinstance(raw_diagnostics, dict) else {}

        if include_personal:
            for k, v in personal.items():
                kk = str(k or "").strip()
                if kk:
                    out[kk] = v
        if include_pins:
            for k, v in pins.items():
                kk = str(k or "").strip()
                if kk:
                    out[kk] = v
        for metric_key, v in metrics.items():
            mapped = registry.column_for_key(str(metric_key))
            if mapped is None:
                continue
            col, _typ = mapped
            out[col] = v
        if include_diagnostics:
            for k, v in diagnostics.items():
                kk = str(k or "").strip()
                if kk:
                    out[kk] = v

        out_rows.append(out)
    return pd.DataFrame(out_rows)
