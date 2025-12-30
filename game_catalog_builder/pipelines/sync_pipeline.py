from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import cast

import pandas as pd

from ..metrics.jsonl import index_jsonl_by_row_id, load_jsonl_strict, write_jsonl
from ..metrics.registry import MetricsRegistry, load_metrics_registry
from ..utils import ensure_columns, ensure_row_ids, read_csv
from .common import write_full_csv


def _default_metrics_registry_path() -> Path:
    from ..metrics.registry import default_metrics_registry_path

    return default_metrics_registry_path()


def _diagnostic_columns(registry: MetricsRegistry) -> set[str]:
    return set(registry.diagnostic_columns) | {"NeedsReview"}


def _drop_eval_columns(df: pd.DataFrame, *, diagnostic_columns: set[str]) -> pd.DataFrame:
    cols = [c for c in sorted(diagnostic_columns) if c in df.columns]
    return df.drop(columns=cols) if cols else df


def sync_back_catalog(
    *,
    catalog_csv: Path,
    enriched_csv: Path,
    output_csv: Path,
    deleted_mode: str = "disable",
    internal_jsonl: Path | None = None,
) -> Path:
    registry = load_metrics_registry(_default_metrics_registry_path())
    diagnostic_columns = _diagnostic_columns(registry)

    catalog = read_csv(catalog_csv)
    enriched = _read_enriched_any(enriched_csv)
    if "RowId" not in enriched.columns:
        enriched = ensure_columns(enriched, {"RowId": ""})
    enriched, created = ensure_row_ids(enriched)
    if created:
        logging.info(f"ℹ sync: generated RowIds for {created} new enriched rows")

    if "RowId" not in catalog.columns:
        raise SystemExit(f"Catalog is missing RowId column: {catalog_csv}")

    catalog["RowId"] = catalog["RowId"].astype(str).str.strip()
    enriched["RowId"] = enriched["RowId"].astype(str).str.strip()

    provider_prefixes = ("RAWG_", "IGDB_", "Steam_", "SteamSpy_", "HLTB_", "Wikidata_")
    provider_id_cols = {
        "RAWG_ID",
        "IGDB_ID",
        "Steam_AppID",
        "HLTB_ID",
        "HLTB_Query",
        "Wikidata_QID",
    }
    always_keep = {"RowId", "Name"} | provider_id_cols

    sync_cols: list[str] = []
    for c in enriched.columns:
        if c in always_keep:
            sync_cols.append(c)
            continue
        if c.startswith(provider_prefixes):
            continue
        if c.startswith("__"):
            continue
        sync_cols.append(c)

    e_idx = enriched.set_index("RowId", drop=False)
    c_idx = catalog.set_index("RowId", drop=False)

    missing_in_enriched = [rid for rid in c_idx.index.tolist() if rid not in e_idx.index]
    if missing_in_enriched:
        if deleted_mode == "disable":
            if "Disabled" not in c_idx.columns:
                c_idx["Disabled"] = ""
            c_idx.loc[missing_in_enriched, "Disabled"] = "YES"
        elif deleted_mode == "drop":
            c_idx = c_idx.drop(index=missing_in_enriched)
        else:
            raise ValueError(f"Unknown deleted_mode: {deleted_mode}")

    for col in sync_cols:
        if col == "RowId":
            continue
        if col not in c_idx.columns:
            c_idx[col] = ""
        values = e_idx[col] if col in e_idx.columns else pd.Series([], dtype=object)
        common = c_idx.index.intersection(e_idx.index)
        c_idx.loc[common, col] = values.loc[common].values

    added = [rid for rid in e_idx.index.tolist() if rid not in c_idx.index]
    if added:
        add_rows = e_idx.loc[added].copy()
        add_out = pd.DataFrame(index=add_rows.index)
        for col in c_idx.columns:
            add_out[col] = ""
        for col in sync_cols:
            if col == "RowId":
                continue
            if col not in add_rows.columns:
                continue
            if col not in add_out.columns:
                add_out[col] = ""
            add_out[col] = add_rows[col].values
        add_out["RowId"] = add_rows["RowId"].values
        c_idx = pd.concat([c_idx, add_out[c_idx.columns]], axis=0)

    out = c_idx.reset_index(drop=True)
    out = _drop_eval_columns(cast(pd.DataFrame, out), diagnostic_columns=diagnostic_columns)
    write_full_csv(out, output_csv)

    if internal_jsonl is not None:
        _sync_internal_jsonl(
            enriched=enriched,
            internal_jsonl=internal_jsonl,
            diagnostic_columns=diagnostic_columns,
        )

    logging.info(
        f"✔ sync updated catalog: {output_csv} (synced_cols={len(sync_cols)}, "
        f"added={len(added)}, deleted={len(missing_in_enriched)})"
    )
    return output_csv


def _read_enriched_any(path: Path) -> pd.DataFrame:
    """
    Read an edited enriched export in either CSV or JSON (array-of-objects).
    """
    if path.suffix.lower() != ".json":
        return read_csv(path)
    data = json.loads(path.read_text(encoding="utf-8") or "[]")
    if not isinstance(data, list):
        raise ValueError(f"Expected JSON array in {path}")
    rows: list[dict[str, object]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        rows.append(item)
    return pd.DataFrame(rows)


def _sync_internal_jsonl(
    *, enriched: pd.DataFrame, internal_jsonl: Path, diagnostic_columns: set[str]
) -> None:
    """
    Update `personal` + `pins` in the internal JSONL store using the edited enriched CSV.

    This does NOT overwrite provider/derived metrics; those are regenerated by `enrich`.
    """
    existing_rows = load_jsonl_strict(internal_jsonl)
    by_id = index_jsonl_by_row_id(existing_rows)

    provider_prefixes = ("RAWG_", "IGDB_", "Steam_", "SteamSpy_", "HLTB_", "Wikidata_")
    pin_cols = {
        "RAWG_ID",
        "IGDB_ID",
        "Steam_AppID",
        "HLTB_ID",
        "HLTB_Query",
        "Wikidata_QID",
    }

    enriched = enriched.copy()
    if "RowId" not in enriched.columns:
        enriched["RowId"] = ""
    enriched["RowId"] = enriched["RowId"].astype(str).str.strip()
    mask = enriched["RowId"].astype(str).str.strip() != ""
    enriched = cast(pd.DataFrame, enriched[mask])

    # Determine which personal columns are allowed to sync back (exclude provider columns).
    personal_cols: list[str] = []
    for c in enriched.columns:
        if c == "RowId":
            continue
        if c in diagnostic_columns:
            continue
        if c in pin_cols:
            continue
        if c.startswith(provider_prefixes):
            continue
        if c.startswith("__"):
            continue
        personal_cols.append(c)

    out_rows: list[dict[str, object]] = []
    for _, r in enriched.iterrows():
        row_id = str(r.get("RowId", "") or "").strip()
        if not row_id:
            continue
        obj = by_id.get(row_id)
        if not isinstance(obj, dict):
            obj = {
                "row_id": row_id,
                "personal": {},
                "pins": {},
                "metrics": {},
                "diagnostics": {},
                "meta": {},
            }
        raw_personal = obj.get("personal")
        personal: dict[str, object] = (
            cast(dict[str, object], raw_personal) if isinstance(raw_personal, dict) else {}
        )
        raw_pins = obj.get("pins")
        pins: dict[str, object] = cast(dict[str, object], raw_pins) if isinstance(raw_pins, dict) else {}

        for c in personal_cols:
            v = str(r.get(c, "") or "").strip()
            if v:
                personal[c] = v
            else:
                personal.pop(c, None)

        for c in sorted(pin_cols):
            if c not in enriched.columns:
                continue
            v = str(r.get(c, "") or "").strip()
            if v:
                pins[c] = v
            else:
                pins.pop(c, None)

        obj["personal"] = personal
        obj["pins"] = pins
        out_rows.append(obj)

    internal_jsonl.parent.mkdir(parents=True, exist_ok=True)
    write_jsonl(out_rows, internal_jsonl)
    logging.info(f"✔ sync updated internal JSONL: {internal_jsonl}")
