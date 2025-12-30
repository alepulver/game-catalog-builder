from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from ..metrics.csv_render import to_csv_cell
from ..metrics.jsonl import (
    load_jsonl_strict,
    validate_jsonl_rows_against_registry,
)
from ..metrics.registry import MetricsRegistry, default_metrics_registry_path, load_metrics_registry
from ..utils.utilities import write_csv


def _read_registry(run_dir: Path, metrics_registry_path: Path | None) -> MetricsRegistry:
    if metrics_registry_path is not None:
        return load_metrics_registry(metrics_registry_path)
    return load_metrics_registry(default_metrics_registry_path(run_dir=run_dir))


def _manifest_path_for_jsonl(p: Path) -> Path:
    if p.suffix != ".jsonl":
        return p.with_suffix(p.suffix + ".manifest.json")
    return p.with_suffix("").with_suffix(".manifest.json")


def _read_manifest_columns(jsonl_path: Path) -> list[str] | None:
    mp = _manifest_path_for_jsonl(jsonl_path)
    if not mp.exists():
        return None
    try:
        data = json.loads(mp.read_text(encoding="utf-8") or "{}")
    except Exception:
        return None
    cols = data.get("columns")
    if not isinstance(cols, list):
        return None
    out: list[str] = []
    for c in cols:
        s = str(c or "").strip()
        if s:
            out.append(s)
    return out or None


def _read_manifest_column_to_key(jsonl_path: Path) -> dict[str, str]:
    mp = _manifest_path_for_jsonl(jsonl_path)
    if not mp.exists():
        return {}
    try:
        data = json.loads(mp.read_text(encoding="utf-8") or "{}")
    except Exception:
        return {}
    m = data.get("column_to_metric_key")
    if not isinstance(m, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in m.items():
        ck = str(k or "").strip()
        cv = str(v or "").strip()
        if ck and cv:
            out[ck] = cv
    return out


def _row_get(
    row: dict[str, Any],
    *,
    col: str,
    registry: MetricsRegistry,
    column_to_key: dict[str, str] | None = None,
) -> object | None:
    if col == "RowId":
        return str(row.get("row_id", "") or "").strip()

    raw_personal = row.get("personal")
    raw_pins = row.get("pins")
    raw_metrics = row.get("metrics")
    personal: dict[str, Any] = raw_personal if isinstance(raw_personal, dict) else {}
    pins: dict[str, Any] = raw_pins if isinstance(raw_pins, dict) else {}
    metrics: dict[str, Any] = raw_metrics if isinstance(raw_metrics, dict) else {}

    if col in personal:
        return personal.get(col)
    if col in pins:
        return pins.get(col)

    if column_to_key and col in column_to_key:
        return metrics.get(column_to_key[col])

    mapped = registry.key_for_column(col)
    if mapped is None:
        return None
    key, _typ = mapped
    return metrics.get(key)


def export_enriched_json(
    *,
    run_dir: Path,
    input_jsonl: Path,
    output_json: Path,
    metrics_registry: Path | None = None,
) -> None:
    """
    Export a user-friendly JSON array using the same columns/order as the CSV export.
    Values preserve JSON types where possible (e.g., arrays stay arrays).
    """
    registry = _read_registry(run_dir, metrics_registry)
    rows = load_jsonl_strict(input_jsonl)
    validate_jsonl_rows_against_registry(rows, registry=registry, context=f"export {input_jsonl.name}")
    if not rows:
        raise SystemExit(f"No JSONL rows found: {input_jsonl}")

    columns = _read_manifest_columns(input_jsonl)
    if not columns:
        raise SystemExit(
            "Missing JSONL manifest columns for export. Expected: "
            f"{_manifest_path_for_jsonl(input_jsonl)}. "
            "Re-run `enrich` so JSONL manifests are written."
        )

    column_to_key = _read_manifest_column_to_key(input_jsonl)
    out_rows: list[dict[str, object]] = []
    for r in rows:
        row_id = str(r.get("row_id", "") or "").strip()
        if not row_id:
            continue
        out: dict[str, object] = {}
        for c in columns:
            v = _row_get(r, col=c, registry=registry, column_to_key=column_to_key)
            if v is None:
                continue
            out[c] = v
        out_rows.append(out)

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(out_rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    logging.info(f"✔ Exported JSON: {output_json}")


def export_enriched_jsonl_view(
    *,
    run_dir: Path,
    input_jsonl: Path,
    output_jsonl: Path,
    metrics_registry: Path | None = None,
) -> None:
    """
    Export a JSONL "view" (one JSON object per line) using the same columns/order as the CSV
    export. Values preserve JSON types where possible (e.g., arrays stay arrays).
    """
    registry = _read_registry(run_dir, metrics_registry)
    rows = load_jsonl_strict(input_jsonl)
    validate_jsonl_rows_against_registry(rows, registry=registry, context=f"export {input_jsonl.name}")
    if not rows:
        raise SystemExit(f"No JSONL rows found: {input_jsonl}")

    columns = _read_manifest_columns(input_jsonl)
    if not columns:
        raise SystemExit(
            "Missing JSONL manifest columns for export. Expected: "
            f"{_manifest_path_for_jsonl(input_jsonl)}. "
            "Re-run `enrich` so JSONL manifests are written."
        )

    column_to_key = _read_manifest_column_to_key(input_jsonl)
    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with output_jsonl.open("w", encoding="utf-8") as f:
        for r in rows:
            row_id = str(r.get("row_id", "") or "").strip()
            if not row_id:
                continue
            out: dict[str, object] = {}
            for c in columns:
                v = _row_get(r, col=c, registry=registry, column_to_key=column_to_key)
                if v is None:
                    continue
                out[c] = v
            f.write(json.dumps(out, ensure_ascii=False) + "\n")
    logging.info(f"✔ Exported JSONL: {output_jsonl}")


def export_enriched_csv(
    *,
    run_dir: Path,
    input_jsonl: Path,
    output_csv: Path,
    metrics_registry: Path | None = None,
) -> None:
    """
    Export a spreadsheet-friendly enriched CSV view from the internal JSONL.
    """
    registry = _read_registry(run_dir, metrics_registry)
    rows = load_jsonl_strict(input_jsonl)
    validate_jsonl_rows_against_registry(rows, registry=registry, context=f"export {input_jsonl.name}")
    if not rows:
        raise SystemExit(f"No JSONL rows found: {input_jsonl}")

    columns = _read_manifest_columns(input_jsonl)
    if not columns:
        raise SystemExit(
            "Missing JSONL manifest columns for export. Expected: "
            f"{_manifest_path_for_jsonl(input_jsonl)}. "
            "Re-run `enrich` so JSONL manifests are written."
        )
    column_to_key = _read_manifest_column_to_key(input_jsonl)

    out_rows: list[dict[str, str]] = []
    for r in rows:
        row_id = str(r.get("row_id", "") or "").strip()
        if not row_id:
            continue
        out: dict[str, str] = {}
        for c in columns:
            out[c] = to_csv_cell(_row_get(r, col=c, registry=registry, column_to_key=column_to_key))
        out_rows.append(out)

    df = pd.DataFrame(out_rows).reindex(columns=columns)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    write_csv(df, output_csv)
    logging.info(f"✔ Exported CSV: {output_csv}")


def export_provider_csv(
    *,
    run_dir: Path,
    provider: str,
    input_jsonl: Path,
    output_csv: Path,
    metrics_registry: Path | None = None,
) -> None:
    """
    Export a Provider_<provider>.csv view from Provider_<provider>.jsonl.
    """
    prov = provider.strip().lower()
    prefix = {
        "rawg": "RAWG_",
        "igdb": "IGDB_",
        "steam": "Steam_",
        "steamspy": "SteamSpy_",
        "hltb": "HLTB_",
        "wikidata": "Wikidata_",
    }.get(prov)
    if not prefix:
        raise SystemExit(f"Unknown provider: {provider}")

    registry = _read_registry(run_dir, metrics_registry)
    rows = load_jsonl_strict(input_jsonl)
    validate_jsonl_rows_against_registry(rows, registry=registry, context=f"export {input_jsonl.name}")
    if not rows:
        raise SystemExit(f"No JSONL rows found: {input_jsonl}")

    columns = _read_manifest_columns(input_jsonl) or []
    if columns:
        column_to_key = _read_manifest_column_to_key(input_jsonl)
        # Use the original provider output column ordering from the run.
        out_rows: list[dict[str, str]] = []
        for r in rows:
            row_id = str(r.get("row_id", "") or "").strip()
            if not row_id:
                continue
            out: dict[str, str] = {}
            for c in columns:
                out[c] = to_csv_cell(_row_get(r, col=c, registry=registry, column_to_key=column_to_key))
            out_rows.append(out)

        df = pd.DataFrame(out_rows).reindex(columns=columns)
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        write_csv(df, output_csv)
        logging.info(f"✔ Exported CSV: {output_csv}")
        return

    out_rows: list[dict[str, str]] = []
    present_cols: set[str] = set()
    for r in rows:
        row_id = str(r.get("row_id", "") or "").strip()
        if not row_id:
            continue
        out: dict[str, str] = {"RowId": row_id}

        raw_personal = r.get("personal")
        raw_metrics = r.get("metrics")
        personal: dict[str, Any] = raw_personal if isinstance(raw_personal, dict) else {}
        metrics: dict[str, Any] = raw_metrics if isinstance(raw_metrics, dict) else {}

        # Provider outputs always include the catalog Name for joinability.
        if "Name" in personal:
            out["Name"] = to_csv_cell(personal.get("Name"))

        for metric_key, v in metrics.items():
            mapped = registry.column_for_key(str(metric_key))
            if mapped is None:
                continue
            col, _typ = mapped
            if col.startswith(prefix):
                out[col] = to_csv_cell(v)
                present_cols.add(col)
        out_rows.append(out)

    columns: list[str] = ["RowId", "Name"]
    # Keep a stable order based on registry YAML insertion order.
    for _metric_key, (col, _typ) in registry.by_key.items():
        if not col.startswith(prefix):
            continue
        if col in present_cols and col not in columns:
            columns.append(col)

    df = pd.DataFrame(out_rows).reindex(columns=columns)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    write_csv(df, output_csv)
    logging.info(f"✔ Exported CSV: {output_csv}")


def export_provider_json(
    *,
    run_dir: Path,
    provider: str,
    input_jsonl: Path,
    output_json: Path,
    metrics_registry: Path | None = None,
) -> None:
    """
    Export a provider JSON array using the original provider output columns/order.
    """
    registry = _read_registry(run_dir, metrics_registry)
    rows = load_jsonl_strict(input_jsonl)
    validate_jsonl_rows_against_registry(rows, registry=registry, context=f"export {input_jsonl.name}")
    if not rows:
        raise SystemExit(f"No JSONL rows found: {input_jsonl}")

    columns = _read_manifest_columns(input_jsonl)
    if not columns:
        raise SystemExit(
            "Missing JSONL manifest columns for export. Expected: "
            f"{_manifest_path_for_jsonl(input_jsonl)}. "
            "Re-run `enrich` so JSONL manifests are written."
        )

    column_to_key = _read_manifest_column_to_key(input_jsonl)
    out_rows: list[dict[str, object]] = []
    for r in rows:
        row_id = str(r.get("row_id", "") or "").strip()
        if not row_id:
            continue
        out: dict[str, object] = {}
        for c in columns:
            v = _row_get(r, col=c, registry=registry, column_to_key=column_to_key)
            if v is None:
                continue
            out[c] = v
        out_rows.append(out)

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(out_rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    logging.info(f"✔ Exported JSON: {output_json}")


def export_provider_jsonl_view(
    *,
    run_dir: Path,
    provider: str,
    input_jsonl: Path,
    output_jsonl: Path,
    metrics_registry: Path | None = None,
) -> None:
    """
    Export a provider JSONL "view" (one JSON object per line) using the original provider output
    columns/order.
    """
    registry = _read_registry(run_dir, metrics_registry)
    rows = load_jsonl_strict(input_jsonl)
    validate_jsonl_rows_against_registry(rows, registry=registry, context=f"export {input_jsonl.name}")
    if not rows:
        raise SystemExit(f"No JSONL rows found: {input_jsonl}")

    columns = _read_manifest_columns(input_jsonl)
    if not columns:
        raise SystemExit(
            "Missing JSONL manifest columns for export. Expected: "
            f"{_manifest_path_for_jsonl(input_jsonl)}. "
            "Re-run `enrich` so JSONL manifests are written."
        )

    column_to_key = _read_manifest_column_to_key(input_jsonl)
    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with output_jsonl.open("w", encoding="utf-8") as f:
        for r in rows:
            row_id = str(r.get("row_id", "") or "").strip()
            if not row_id:
                continue
            out: dict[str, object] = {}
            for c in columns:
                v = _row_get(r, col=c, registry=registry, column_to_key=column_to_key)
                if v is None:
                    continue
                out[c] = v
            f.write(json.dumps(out, ensure_ascii=False) + "\n")
    logging.info(f"✔ Exported JSONL: {output_jsonl}")
