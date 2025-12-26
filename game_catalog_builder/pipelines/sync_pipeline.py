from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..schema import EVAL_COLUMNS
from ..utils import ensure_columns, ensure_row_ids, read_csv, write_csv


def _drop_eval_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in EVAL_COLUMNS if c in df.columns]
    return df.drop(columns=cols) if cols else df


def sync_back_catalog(
    *,
    catalog_csv: Path,
    enriched_csv: Path,
    output_csv: Path,
    deleted_mode: str = "disable",
) -> Path:
    catalog = read_csv(catalog_csv)
    enriched = read_csv(enriched_csv)
    if "RowId" not in enriched.columns:
        enriched = ensure_columns(enriched, ["RowId"])
    enriched, created = ensure_row_ids(enriched)
    if created:
        logging.info(f"ℹ sync: generated RowIds for {created} new enriched rows")

    if "RowId" not in catalog.columns:
        raise SystemExit(f"Catalog is missing RowId column: {catalog_csv}")

    catalog["RowId"] = catalog["RowId"].astype(str).str.strip()
    enriched["RowId"] = enriched["RowId"].astype(str).str.strip()

    provider_prefixes = ("RAWG_", "IGDB_", "Steam_", "SteamSpy_", "HLTB_", "Wikidata_")
    provider_id_cols = {"RAWG_ID", "IGDB_ID", "Steam_AppID", "HLTB_ID", "HLTB_Query", "Wikidata_QID"}
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
    out = _drop_eval_columns(out)
    write_csv(out, output_csv)
    logging.info(
        f"✔ sync updated catalog: {output_csv} (synced_cols={len(sync_cols)}, "
        f"added={len(added)}, deleted={len(missing_in_enriched)})"
    )
    return output_csv

