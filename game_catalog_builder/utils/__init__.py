"""Utility functions and helpers."""

from .identity import generate_identity_map, merge_identity_user_fields
from .merger import merge_all
from .utilities import (
    IDENTITY_NOT_FOUND,
    PUBLIC_DEFAULT_COLS,
    ProjectPaths,
    ensure_columns,
    ensure_row_ids,
    ensure_row_ids_in_input,
    fuzzy_score,
    is_row_processed,
    load_credentials,
    load_identity_overrides,
    normalize_game_name,
    pick_best_match,
    read_csv,
    write_csv,
)
from .validation import ValidationThresholds, generate_validation_report

__all__ = [
    "ProjectPaths",
    "IDENTITY_NOT_FOUND",
    "ensure_columns",
    "ensure_row_ids_in_input",
    "ensure_row_ids",
    "load_identity_overrides",
    "fuzzy_score",
    "is_row_processed",
    "load_credentials",
    "merge_all",
    "generate_identity_map",
    "merge_identity_user_fields",
    "generate_validation_report",
    "ValidationThresholds",
    "normalize_game_name",
    "pick_best_match",
    "read_csv",
    "write_csv",
    "PUBLIC_DEFAULT_COLS",
]
