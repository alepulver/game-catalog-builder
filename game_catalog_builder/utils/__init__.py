"""
Utility functions and helpers.

This module intentionally uses lazy attribute loading to avoid importing heavier
submodules (e.g., pandas) unless they are needed.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "ProjectPaths",
    "RunPaths",
    "IDENTITY_NOT_FOUND",
    "extract_year_hint",
    "ensure_columns",
    "ensure_row_ids",
    "load_identity_overrides",
    "fuzzy_score",
    "is_row_processed",
    "load_credentials",
    "load_json_cache",
    "merge_all",
    "generate_validation_report",
    "ValidationThresholds",
    "normalize_game_name",
    "pick_best_match",
    "read_csv",
    "write_csv",
    "PUBLIC_DEFAULT_COLS",
    "ReviewConfig",
    "build_review_csv",
]


def __getattr__(name: str) -> Any:  # pragma: no cover
    if name in {
        "IDENTITY_NOT_FOUND",
        "PUBLIC_DEFAULT_COLS",
        "ProjectPaths",
        "RunPaths",
        "ensure_columns",
        "ensure_row_ids",
        "extract_year_hint",
        "fuzzy_score",
        "is_row_processed",
        "load_credentials",
        "load_identity_overrides",
        "load_json_cache",
        "normalize_game_name",
        "pick_best_match",
        "read_csv",
        "write_csv",
    }:
        from . import utilities as _u

        return getattr(_u, name)

    if name == "merge_all":
        from .merger import merge_all

        return merge_all

    if name in {"ValidationThresholds", "generate_validation_report"}:
        from .validation import ValidationThresholds, generate_validation_report

        return (
            ValidationThresholds if name == "ValidationThresholds" else generate_validation_report
        )

    if name in {"ReviewConfig", "build_review_csv"}:
        from .review import ReviewConfig, build_review_csv

        return ReviewConfig if name == "ReviewConfig" else build_review_csv

    raise AttributeError(name)
