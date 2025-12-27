"""Command-line interface for game catalog builder."""

from __future__ import annotations

import argparse
import logging
import os
import shlex
import sys
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from .pipelines.context import PipelineContext
from .pipelines.enrich_pipeline import run_enrich
from .pipelines.import_pipeline import run_import
from .pipelines.resolve_pipeline import run_resolve
from .pipelines.sync_pipeline import sync_back_catalog
from .schema import (
    ENRICH_ALLOWED_SOURCES,
    IMPORT_ALLOWED_SOURCES,
    RESOLVE_ALLOWED_SOURCES,
    SOURCE_ALIASES,
)
from .utils import (
    ProjectPaths,
    ReviewConfig,
    RunPaths,
    build_review_csv,
    generate_validation_report,
    read_csv,
    write_csv,
)
from .utils.source_selection import parse_sources


def _parse_sources(
    raw: str, *, allowed: set[str], aliases: dict[str, list[str]] | None = None
) -> list[str]:
    return parse_sources(raw, allowed=allowed, aliases=aliases)


def setup_logging(log_file: Path) -> None:
    """Configure logging to both console and file."""
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Create formatters
    file_formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    console_formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # File handler
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Silence verbose HTTP debug logs by default
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    logging.info(f"Logging to file: {log_file}")


def _common_paths() -> tuple[Path, ProjectPaths]:
    project_root = Path(__file__).resolve().parent.parent
    paths = ProjectPaths.from_root(project_root)
    paths.ensure()
    return project_root, paths


def _infer_run_paths(
    *,
    project_root: Path,
    run_dir: Path | None,
    input_path: Path | None,
) -> RunPaths:
    """
    Determine the "run dir" for a command and derive cache/input/output/logs dirs from it.

    Default run dir is `<repo>/data`. If the input path is located under `<run_dir>/input/`,
    that run dir is inferred automatically.
    """
    if run_dir is not None:
        resolved = run_dir
        if not resolved.is_absolute():
            resolved = (project_root / resolved).resolve()
        rp = RunPaths.from_run_dir(resolved)
        rp.ensure()
        return rp

    if input_path is not None:
        try:
            inp = input_path.resolve()
            if inp.parent.name == "input":
                rp = RunPaths.from_run_dir(inp.parent.parent)
                rp.ensure()
                return rp
        except Exception:
            pass

    rp = RunPaths.from_run_dir(project_root / "data")
    rp.ensure()
    return rp


def _apply_run_overrides(
    run_paths: RunPaths,
    *,
    project_root: Path,
    cache_dir: Path | None,
    logs_dir: Path | None,
) -> RunPaths:
    def _abs(p: Path | None) -> Path | None:
        if p is None:
            return None
        return (p if p.is_absolute() else (project_root / p)).resolve()

    cache_dir_r = _abs(cache_dir) or run_paths.cache_dir
    logs_dir_r = _abs(logs_dir) or run_paths.logs_dir
    out = replace(run_paths, cache_dir=cache_dir_r, logs_dir=logs_dir_r)
    out.ensure()
    return out


def _prepare_run_paths(
    *,
    project_root: Path,
    args: argparse.Namespace,
    input_path: Path | None,
    allow_cache_override: bool,
) -> RunPaths:
    run_paths = _infer_run_paths(
        project_root=project_root, run_dir=getattr(args, "run_dir", None), input_path=input_path
    )
    cache_dir = getattr(args, "cache", None) if allow_cache_override else None
    logs_dir = getattr(args, "logs_dir", None)
    return _apply_run_overrides(
        run_paths,
        project_root=project_root,
        cache_dir=cache_dir,
        logs_dir=logs_dir,
    )


def _default_log_file(*, command_name: str, logs_dir: Path) -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    stamp = now.strftime("%Y%m%d-%H%M%S") + f".{now.microsecond // 1000:03d}"
    base = f"log-{stamp}-{command_name}.log"
    candidate = logs_dir / base
    if not candidate.exists():
        return candidate

    for i in range(2, 1000):
        p = logs_dir / f"log-{stamp}-{command_name}-{i}.log"
        if not p.exists():
            return p
    return logs_dir / f"log-{stamp}-{command_name}-{os.getpid()}.log"


def _setup_logging_from_args(
    run_paths: RunPaths,
    log_file: Path | None,
    debug: bool,
    *,
    command_name: str,
) -> None:
    setup_logging(
        log_file or _default_log_file(command_name=command_name, logs_dir=run_paths.logs_dir)
    )
    if debug:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        for handler in root_logger.handlers:
            handler.setLevel(logging.DEBUG)
        logging.getLogger("urllib3").setLevel(logging.DEBUG)

    argv = " ".join(shlex.quote(a) for a in sys.argv)
    logging.info(f"Invocation: {argv}")


def _command_normalize(args: argparse.Namespace) -> None:
    project_root, paths = _common_paths()
    run_paths = _prepare_run_paths(
        project_root=project_root,
        args=args,
        input_path=args.input,
        allow_cache_override=True,
    )
    _setup_logging_from_args(run_paths, args.log_file, args.debug, command_name="import")

    cache_dir = run_paths.cache_dir
    cache_dir.mkdir(parents=True, exist_ok=True)

    out = args.out or (run_paths.input_dir / "Games_Catalog.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    include_diagnostics = bool(args.diagnostics)

    credentials_path = args.credentials or (project_root / "data" / "credentials.yaml")
    sources = _parse_sources(
        args.source, allowed=set(IMPORT_ALLOWED_SOURCES), aliases=SOURCE_ALIASES
    )
    ctx = PipelineContext(cache_dir=cache_dir, credentials_path=credentials_path, sources=sources)
    run_import(ctx, input_csv=args.input, output_csv=out, include_diagnostics=include_diagnostics)
    return


def _command_resolve(args: argparse.Namespace) -> None:
    project_root, _paths = _common_paths()
    run_paths = _prepare_run_paths(
        project_root=project_root,
        args=args,
        input_path=getattr(args, "catalog", None),
        allow_cache_override=True,
    )
    _setup_logging_from_args(run_paths, args.log_file, args.debug, command_name="resolve")

    cache_dir = run_paths.cache_dir
    cache_dir.mkdir(parents=True, exist_ok=True)

    catalog_csv = args.catalog or (run_paths.input_dir / "Games_Catalog.csv")
    if not catalog_csv.exists():
        raise SystemExit(f"Catalog file not found: {catalog_csv}")

    out = args.out or catalog_csv
    out.parent.mkdir(parents=True, exist_ok=True)

    credentials_path = args.credentials or (project_root / "data" / "credentials.yaml")

    apply = bool(getattr(args, "apply", False))

    sources = _parse_sources(
        args.source, allowed=set(RESOLVE_ALLOWED_SOURCES), aliases=SOURCE_ALIASES
    )
    ctx = PipelineContext(cache_dir=cache_dir, credentials_path=credentials_path, sources=sources)
    stats = run_resolve(
        ctx,
        catalog_csv=catalog_csv,
        out_csv=out,
        retry_missing=bool(args.retry_missing),
        apply=apply,
    )
    logging.info(
        f"✔ Resolve completed: {out} (apply={str(apply).lower()}, attempted={stats.attempted}, "
        f"repinned={stats.repinned}, unpinned={stats.unpinned}, kept={stats.kept}, "
        f"wikidata_hint_added={stats.wikidata_hint_added})"
    )
    return


def _command_enrich(args: argparse.Namespace) -> None:
    project_root, _paths = _common_paths()
    run_paths = _prepare_run_paths(
        project_root=project_root,
        args=args,
        input_path=args.input,
        allow_cache_override=True,
    )
    _setup_logging_from_args(run_paths, args.log_file, args.debug, command_name="enrich")
    logging.info("Starting game catalog enrichment")

    input_csv = args.input
    if not input_csv.exists():
        raise SystemExit(f"Input file not found: {input_csv}")

    output_dir = args.output or run_paths.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = run_paths.cache_dir
    cache_dir.mkdir(parents=True, exist_ok=True)

    merge_output = args.merge_output or (output_dir / "Games_Enriched.csv")

    sources_to_process = _parse_sources(
        args.source,
        allowed=set(ENRICH_ALLOWED_SOURCES),
        aliases=SOURCE_ALIASES,
    )
    credentials_path = args.credentials or (project_root / "data" / "credentials.yaml")
    run_enrich(
        input_csv=input_csv,
        output_dir=output_dir,
        cache_dir=cache_dir,
        credentials_path=credentials_path,
        sources=sources_to_process,
        clean_output=bool(args.clean_output),
        merge_output=merge_output,
        validate=bool(args.validate),
        validate_output=args.validate_output,
    )


def _command_sync_back(args: argparse.Namespace) -> None:
    project_root, _paths = _common_paths()
    run_paths = _prepare_run_paths(
        project_root=project_root,
        args=args,
        input_path=args.catalog,
        allow_cache_override=False,
    )
    _setup_logging_from_args(run_paths, args.log_file, args.debug, command_name="sync")
    out = args.out or args.catalog
    sync_back_catalog(catalog_csv=args.catalog, enriched_csv=args.enriched, output_csv=out)
    return


def _command_validate(args: argparse.Namespace) -> None:
    project_root, _paths = _common_paths()
    run_paths = _prepare_run_paths(
        project_root=project_root,
        args=args,
        input_path=getattr(args, "enriched", None),
        allow_cache_override=False,
    )
    _setup_logging_from_args(run_paths, args.log_file, args.debug, command_name="validate")
    output_dir = args.output_dir or run_paths.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    out = args.out or (output_dir / "Validation_Report.csv")
    if args.enriched:
        enriched = read_csv(args.enriched)
    else:
        enriched_path = output_dir / "Games_Enriched.csv"
        if not enriched_path.exists():
            raise SystemExit(
                f"Missing {enriched_path}; run `enrich` first or pass --enriched explicitly"
            )
        enriched = read_csv(enriched_path)
    report = generate_validation_report(enriched)
    write_csv(report, out)
    logging.info(f"✔ Validation report generated: {out}")


def _command_review(args: argparse.Namespace) -> None:
    project_root, _paths = _common_paths()
    run_paths = _prepare_run_paths(
        project_root=project_root,
        args=args,
        input_path=getattr(args, "catalog", None),
        allow_cache_override=False,
    )
    _setup_logging_from_args(run_paths, args.log_file, args.debug, command_name="review")
    catalog_csv = args.catalog or (run_paths.input_dir / "Games_Catalog.csv")
    if not catalog_csv.exists():
        raise SystemExit(f"Catalog not found: {catalog_csv}")
    enriched_csv = args.enriched or (run_paths.output_dir / "Games_Enriched.csv")
    out = args.out or (run_paths.output_dir / "Review_TopRisk.csv")

    catalog_df = read_csv(catalog_csv)
    enriched_df = read_csv(enriched_csv) if enriched_csv.exists() else None
    review = build_review_csv(
        catalog_df,
        enriched_df=enriched_df,
        config=ReviewConfig(max_rows=int(args.max_rows)),
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    write_csv(review, out)
    logging.info(f"✔ Review CSV generated: {out} (rows={len(review)})")


def _command_collect_production_tiers(args: argparse.Namespace) -> None:
    from game_catalog_builder.tools.collect_production_tiers import collect_production_tiers_yaml

    project_root, _paths = _common_paths()
    run_paths = _prepare_run_paths(
        project_root=project_root,
        args=args,
        input_path=args.enriched,
        allow_cache_override=False,
    )
    _setup_logging_from_args(
        run_paths, args.log_file, args.debug, command_name="collect-production-tiers"
    )

    out = args.out or (run_paths.run_dir / "production_tiers.yaml")
    base = args.base or out
    out.parent.mkdir(parents=True, exist_ok=True)
    res = collect_production_tiers_yaml(
        enriched_csv=args.enriched,
        out_yaml=out,
        base_yaml=base,
        min_count=args.min_count,
        max_examples=args.max_examples,
        include_porting_labels=args.include_porting_labels,
        keep_existing=True,
        only_missing=args.only_missing,
    )
    logging.info(
        f"✔ Production tiers YAML updated: {out} "
        f"(publishers={res.publishers_total} developers={res.developers_total})"
    )


def _command_normalize_production_tiers(args: argparse.Namespace) -> None:
    from game_catalog_builder.tools.normalize_production_tiers import (
        normalize_production_tiers_yaml,
    )

    project_root, _paths = _common_paths()
    run_paths = _prepare_run_paths(
        project_root=project_root,
        args=args,
        input_path=args.in_yaml,
        allow_cache_override=False,
    )
    _setup_logging_from_args(
        run_paths, args.log_file, args.debug, command_name="normalize-production-tiers"
    )

    res = normalize_production_tiers_yaml(in_yaml=args.in_yaml, out_yaml=args.out)
    logging.info(
        "✔ Production tiers YAML normalized: "
        f"{args.in_yaml} -> {args.out or args.in_yaml} "
        f"(publishers={res.publishers_in}->{res.publishers_out} "
        f"merged={res.publishers_merged} conflicts={res.publishers_conflicts}; "
        f"developers={res.developers_in}->{res.developers_out} "
        f"merged={res.developers_merged} conflicts={res.developers_conflicts})"
    )


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        raise SystemExit(
            "Missing command. Use one of: "
            "import, enrich, sync, validate, collect-production-tiers. "
            "Run `python run.py --help` for usage."
        )

    parser = argparse.ArgumentParser(description="Enrich video game catalogs with metadata")
    sub = parser.add_subparsers(dest="command", required=True)

    p_common_paths = argparse.ArgumentParser(add_help=False)
    p_common_paths.add_argument(
        "--run-dir",
        type=Path,
        help=(
            "Run directory containing input/output/cache/logs "
            "(default: infer from input, else ./data)"
        ),
    )
    p_common_paths.add_argument(
        "--logs-dir",
        type=Path,
        help="Override logs directory (default: <run-dir>/logs)",
    )

    p_import = sub.add_parser(
        "import",
        help="Normalize an exported user CSV into Games_Catalog.csv",
        parents=[p_common_paths],
    )
    p_import.add_argument("input", type=Path, help="Input CSV (exported from spreadsheet)")
    p_import.add_argument(
        "--out",
        type=Path,
        help="Output catalog CSV (default: data/input/Games_Catalog.csv)",
    )
    p_import.add_argument(
        "--log-file",
        type=Path,
        help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
    )
    p_import.add_argument("--cache", type=Path, help="Cache directory (default: data/cache)")
    p_import.add_argument(
        "--credentials", type=Path, help="Credentials YAML (default: data/credentials.yaml)"
    )
    p_import.add_argument(
        "--source",
        type=str,
        default="all",
        help=("Which providers to match for IDs (e.g. 'core' or 'igdb,rawg,steam') (default: all)"),
    )
    p_import.add_argument(
        "--diagnostics",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include match diagnostics columns in the output (default: true)",
    )
    p_import.add_argument(
        "--debug", action="store_true", help="Enable DEBUG logging (default: INFO)"
    )
    p_import.set_defaults(_fn=_command_normalize)

    p_resolve = sub.add_parser(
        "resolve",
        help=(
            "Optional third pass: repin-or-unpin likely-wrong IDs and conservatively retry "
            "repinning using consensus titles/aliases"
        ),
        parents=[p_common_paths],
    )
    p_resolve.add_argument(
        "--catalog",
        type=Path,
        help="Catalog CSV with diagnostics (default: <run-dir>/input/Games_Catalog.csv)",
    )
    p_resolve.add_argument(
        "--out",
        type=Path,
        help="Output catalog CSV (default: overwrite --catalog)",
    )
    p_resolve.add_argument(
        "--apply",
        action="store_true",
        help="Write any repin/unpin changes to disk (default: dry-run)",
    )
    p_resolve.add_argument("--cache", type=Path, help="Cache directory (default: data/cache)")
    p_resolve.add_argument(
        "--credentials", type=Path, help="Credentials YAML (default: data/credentials.yaml)"
    )
    p_resolve.add_argument(
        "--source",
        type=str,
        default="core,wikidata",
        help="Providers to use for retries (default: core,wikidata)",
    )
    p_resolve.add_argument(
        "--retry-missing",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Also attempt to fill missing provider IDs when a strict consensus exists "
            "(default: false)"
        ),
    )
    p_resolve.add_argument(
        "--log-file",
        type=Path,
        help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
    )
    p_resolve.add_argument(
        "--debug", action="store_true", help="Enable DEBUG logging (default: INFO)"
    )
    p_resolve.set_defaults(_fn=_command_resolve)

    p_enrich = sub.add_parser(
        "enrich",
        help="Generate provider outputs + Games_Enriched.csv from Games_Catalog.csv",
        parents=[p_common_paths],
    )
    p_enrich.add_argument("input", type=Path, help="Catalog CSV (source of truth)")
    p_enrich.add_argument("--output", type=Path, help="Output directory (default: data/output)")
    p_enrich.add_argument("--cache", type=Path, help="Cache directory (default: data/cache)")
    p_enrich.add_argument(
        "--credentials", type=Path, help="Credentials YAML (default: data/credentials.yaml)"
    )
    p_enrich.add_argument("--source", type=str, default="all")
    p_enrich.add_argument(
        "--clean-output",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Delete and regenerate provider/output CSVs (default: true)",
    )
    p_enrich.add_argument(
        "--merge-output",
        type=Path,
        help="Output file for merged results (default: data/output/Games_Enriched.csv)",
    )
    p_enrich.add_argument(
        "--validate", action="store_true", help="Generate validation report (default: off)"
    )
    p_enrich.add_argument(
        "--validate-output",
        type=Path,
        help="Output file for validation report (default: data/output/Validation_Report.csv)",
    )
    p_enrich.add_argument(
        "--log-file",
        type=Path,
        help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
    )
    p_enrich.add_argument(
        "--debug", action="store_true", help="Enable DEBUG logging (default: INFO)"
    )
    p_enrich.set_defaults(_fn=_command_enrich)

    p_sync = sub.add_parser(
        "sync",
        help="Sync user-editable fields from Games_Enriched.csv back into Games_Catalog.csv",
        parents=[p_common_paths],
    )
    p_sync.add_argument("catalog", type=Path, help="Catalog CSV to update")
    p_sync.add_argument("enriched", type=Path, help="Edited enriched CSV")
    p_sync.add_argument("--out", type=Path, help="Output catalog CSV (default: overwrite catalog)")
    p_sync.add_argument(
        "--log-file",
        type=Path,
        help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
    )
    p_sync.add_argument("--debug", action="store_true", help="Enable DEBUG logging (default: INFO)")
    p_sync.set_defaults(_fn=_command_sync_back)

    p_val = sub.add_parser(
        "validate",
        help="Generate validation report from an enriched CSV (read-only)",
        parents=[p_common_paths],
    )
    p_val.add_argument(
        "--enriched",
        type=Path,
        help="Enriched CSV to validate (default: data/output/Games_Enriched.csv)",
    )
    p_val.add_argument("--output-dir", type=Path, help="Output directory (default: data/output)")
    p_val.add_argument(
        "--out",
        type=Path,
        help="Output validation report path (default: <output-dir>/Validation_Report.csv)",
    )
    p_val.add_argument(
        "--log-file",
        type=Path,
        help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
    )
    p_val.add_argument("--debug", action="store_true", help="Enable DEBUG logging (default: INFO)")
    p_val.set_defaults(_fn=_command_validate)

    p_review = sub.add_parser(
        "review",
        help="Generate a focused review CSV from Games_Catalog.csv (+ optional Games_Enriched.csv)",
        parents=[p_common_paths],
    )
    p_review.add_argument(
        "--catalog",
        type=Path,
        help="Catalog CSV with diagnostics (default: <run-dir>/input/Games_Catalog.csv)",
    )
    p_review.add_argument(
        "--enriched",
        type=Path,
        help=(
            "Enriched CSV (optional; used to add extra context) "
            "(default: <run-dir>/output/Games_Enriched.csv)"
        ),
    )
    p_review.add_argument(
        "--out",
        type=Path,
        help="Output review CSV path (default: <run-dir>/output/Review_TopRisk.csv)",
    )
    p_review.add_argument(
        "--max-rows",
        type=int,
        default=200,
        help="Max rows to include (default: 200)",
    )
    p_review.add_argument(
        "--log-file",
        type=Path,
        help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
    )
    p_review.add_argument(
        "--debug", action="store_true", help="Enable DEBUG logging (default: INFO)"
    )
    p_review.set_defaults(_fn=_command_review)

    p_tiers = sub.add_parser(
        "collect-production-tiers",
        help="Collect publisher/developer tiers candidates from an enriched CSV into a YAML file",
        parents=[p_common_paths],
    )
    p_tiers.add_argument(
        "enriched",
        type=Path,
        help="Enriched CSV (must contain at least one provider *_Publishers/_Developers column)",
    )
    p_tiers.add_argument(
        "--out",
        type=Path,
        help="Output YAML path (default: <run-dir>/production_tiers.yaml)",
    )
    p_tiers.add_argument(
        "--base",
        type=Path,
        help="Base YAML to read existing tiers from (default: --out)",
    )
    p_tiers.add_argument(
        "--min-count",
        type=int,
        default=1,
        help="Only include entities appearing in >= N rows (default: 1)",
    )
    p_tiers.add_argument(
        "--max-examples",
        type=int,
        default=6,
        help="Max example game names per entity (default: 6)",
    )
    p_tiers.add_argument(
        "--only-missing",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Only include entities with missing tiers in the existing YAML (default: false)",
    )
    p_tiers.add_argument(
        "--include-porting-labels",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include porting-label entities (e.g., Aspyr/Feral) (default: true)",
    )
    p_tiers.add_argument(
        "--log-file",
        type=Path,
        help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
    )
    p_tiers.add_argument(
        "--debug", action="store_true", help="Enable DEBUG logging (default: INFO)"
    )
    p_tiers.set_defaults(_fn=_command_collect_production_tiers)

    p_tiers_norm = sub.add_parser(
        "normalize-production-tiers",
        help="Deduplicate/normalize a production tiers YAML by normalized company key",
        parents=[p_common_paths],
    )
    p_tiers_norm.add_argument("in_yaml", type=Path, help="Input production tiers YAML")
    p_tiers_norm.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output YAML path (default: in-place overwrite of input)",
    )
    p_tiers_norm.add_argument(
        "--log-file",
        type=Path,
        help="Log file path (default: data/logs/log-<timestamp>-<command>.log)",
    )
    p_tiers_norm.add_argument(
        "--debug", action="store_true", help="Enable DEBUG logging (default: INFO)"
    )
    p_tiers_norm.set_defaults(_fn=_command_normalize_production_tiers)

    ns = parser.parse_args(argv)
    ns._fn(ns)
    return


if __name__ == "__main__":
    main()
