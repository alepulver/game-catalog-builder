from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Mapping

import pandas as pd

from ..metrics.jsonl import (
    jsonl_rows_to_catalog_dataframe,
    jsonl_rows_to_registered_dataframe,
    load_jsonl_strict,
    validate_jsonl_rows_against_registry,
    write_dataframe_jsonl,
)
from ..metrics.registry import MetricsRegistry


class ArtifactStore:
    """
    Thin helper to make JSONL behave like a transparent, typed cache:
    - Reuse JSONL when present/valid.
    - Otherwise compute via a provided callable and optionally persist JSONL.

    This keeps pipelines free of scattered --reuse-jsonl / --no-jsonl branches.
    """

    def __init__(
        self,
        *,
        run_dir: Path,
        registry: MetricsRegistry,
        use_jsonl: bool,
        reuse_jsonl: bool,
        jsonl_dir: Path,
    ) -> None:
        self.run_dir = run_dir
        self.registry = registry
        self.use_jsonl = use_jsonl
        self.reuse_jsonl = reuse_jsonl and use_jsonl
        self.jsonl_dir = jsonl_dir

    # ------------------
    # Catalog
    # ------------------
    def load_catalog(self, catalog_csv: Path) -> pd.DataFrame:
        """
        Load catalog from JSONL (if enabled/exists) else CSV.
        """
        if self.use_jsonl:
            catalog_jsonl = catalog_csv.parent / "jsonl" / f"{catalog_csv.stem}.jsonl"
            if catalog_jsonl.exists():
                rows = load_jsonl_strict(catalog_jsonl)
                validate_jsonl_rows_against_registry(
                    rows, registry=self.registry, context=str(catalog_jsonl.name)
                )
                return jsonl_rows_to_catalog_dataframe(rows, include_diagnostics=True)
        import pandas as pd  # local import to keep dependency surface small

        return pd.read_csv(catalog_csv)

    def write_catalog(self, df: pd.DataFrame, catalog_csv: Path) -> None:
        """
        Write catalog CSV and, if enabled, the adjacent catalog JSONL.
        """
        from ..pipelines.common import write_full_csv

        write_full_csv(df, catalog_csv)
        if not self.use_jsonl:
            return
        catalog_jsonl = catalog_csv.parent / "jsonl" / f"{catalog_csv.stem}.jsonl"
        write_dataframe_jsonl(
            df,
            path=catalog_jsonl,
            registry=self.registry,
            include_diagnostics=True,
            provider_name=None,
        )

    # ------------------
    # Provider
    # ------------------
    def load_provider_jsonl(self, provider: str) -> pd.DataFrame | None:
        """
        Load a provider frame from JSONL if reuse is enabled and the file exists/validates.
        """
        if not self.reuse_jsonl:
            return None
        path = self.jsonl_dir / f"Provider_{provider}.jsonl"
        if not path.exists():
            return None
        rows = load_jsonl_strict(path)
        validate_jsonl_rows_against_registry(
            rows, registry=self.registry, context=f"provider jsonl reuse ({provider})"
        )
        df = jsonl_rows_to_registered_dataframe(
            rows,
            registry=self.registry,
            include_personal=True,
            include_pins=True,
            include_diagnostics=False,
        )
        if "RowId" not in df.columns:
            return None
        logging.info(f"✔ Reused provider JSONL: {path}")
        return df

    def _overlay(self, base: pd.DataFrame, other: pd.DataFrame) -> pd.DataFrame:
        """
        Overlay `other` onto `base` by RowId (non-empty values overwrite).
        """
        if "RowId" not in base.columns:
            return other
        if "RowId" not in other.columns:
            return base
        merged = base.merge(other, on="RowId", how="outer", suffixes=("", "__new"))
        for c in list(merged.columns):
            if not c.endswith("__new"):
                continue
            orig = c[: -len("__new")]
            if orig in merged.columns:
                mask = merged[c].astype(str).str.strip().ne("")
                merged.loc[mask, orig] = merged.loc[mask, c]
            merged = merged.drop(columns=[c])
        return merged

    def ensure_provider(
        self,
        provider: str,
        *,
        compute: Callable[[], pd.DataFrame],
        compute_missing: Callable[[set[str]], pd.DataFrame] | None = None,
        expected_row_ids: set[str] | None = None,
        include_all_metrics: bool,
    ) -> pd.DataFrame:
        """
        Get provider frame via JSONL reuse or compute+persist.
        """
        reused = self.load_provider_jsonl(provider)
        if reused is not None and expected_row_ids:
            present = {str(rid).strip() for rid in reused.get("RowId", pd.Series([], dtype=str)).tolist()}
            missing = {rid for rid in expected_row_ids if rid and rid not in present}
            if missing and compute_missing is not None:
                df_missing = compute_missing(missing)
                reused = self._overlay(reused, df_missing)
        if reused is not None:
            df = reused
        else:
            df = compute()
        if self.use_jsonl:
            write_dataframe_jsonl(
                df,
                path=self.jsonl_dir / f"Provider_{provider}.jsonl",
                registry=self.registry,
                include_diagnostics=False,
                provider_name=provider,
                include_all_metrics=include_all_metrics,
            )
        return df

    # ------------------
    # Enriched
    # ------------------
    def write_enriched(
        self,
        merged_df: pd.DataFrame,
        *,
        provider_frames: Mapping[str, pd.DataFrame],
        include_all_metrics: bool,
        export_json: bool,
        merge_output: Path | None,
        metrics_registry_path: Path,
    ) -> None:
        """
        Persist merged/provider JSONL (and re-render CSV via export) when enabled.
        """
        from ..pipelines.export_pipeline import export_enriched_csv, export_provider_csv
        from ..metrics.jsonl import write_dataframe_json

        if not self.use_jsonl:
            return

        jsonl_dir = self.jsonl_dir
        json_dir = jsonl_dir.parent / "json"
        write_dataframe_jsonl(
            merged_df,
            path=jsonl_dir / "Games_Enriched.jsonl",
            registry=self.registry,
            include_diagnostics=False,
            provider_name=None,
            include_all_metrics=include_all_metrics,
        )
        if export_json:
            write_dataframe_json(
                merged_df,
                path=json_dir / "Games_Enriched.json",
                registry=self.registry,
                include_diagnostics=False,
                provider_name=None,
            )

        for prov, dfp in provider_frames.items():
            write_dataframe_jsonl(
                dfp,
                path=jsonl_dir / f"Provider_{prov}.jsonl",
                registry=self.registry,
                include_diagnostics=False,
                provider_name=prov,
                include_all_metrics=include_all_metrics,
            )

        # Re-render CSV outputs from JSONL to enforce registry column selection.
        if merge_output is not None:
            export_enriched_csv(
                run_dir=self.run_dir,
                input_jsonl=jsonl_dir / "Games_Enriched.jsonl",
                output_csv=merge_output,
                metrics_registry=metrics_registry_path,
            )
        for prov, dfp in provider_frames.items():
            jsonl_path = jsonl_dir / f"Provider_{prov}.jsonl"
            if not jsonl_path.exists():
                continue
            out_csv = jsonl_dir.parent / f"Provider_{prov.upper() if prov.islower() else prov}.csv"
            export_provider_csv(
                run_dir=self.run_dir,
                provider=prov,
                input_jsonl=jsonl_path,
                output_csv=out_csv,
                metrics_registry=metrics_registry_path,
            )
