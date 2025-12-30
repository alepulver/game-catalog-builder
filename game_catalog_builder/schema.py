from __future__ import annotations

from typing import Any

# -----------------------------------------------------------------------------
# CSV schema / column sets
# -----------------------------------------------------------------------------

# Minimal "required columns" for dataframe merges/loading.
#
# We intentionally do NOT list all provider/public columns here anymore:
# - the metrics registry is the canonical mapping (metric key -> CSV column),
# - missing provider/derived columns should be created on demand when applying metrics.
#
# This set exists only to ensure stable RowId/Name + pin columns are present when loading CSVs.
PUBLIC_DEFAULT_COLS: dict[str, Any] = {
    "RowId": "",
    "Name": "",
    # Provider pins
    "RAWG_ID": "",
    "IGDB_ID": "",
    "Steam_AppID": "",
    "HLTB_ID": "",
    "HLTB_Query": "",
    "Wikidata_QID": "",
}

# Provider column prefixes used to strip derived/public fields on in-place enrich.
PROVIDER_PREFIXES = ("RAWG_", "IGDB_", "Steam_", "SteamSpy_", "HLTB_", "Wikidata_")

# CLI/provider selection
SOURCE_ALIASES: dict[str, list[str]] = {"core": ["igdb", "rawg", "steam"]}
IMPORT_ALLOWED_SOURCES = {"igdb", "rawg", "steam", "hltb", "wikidata"}
ENRICH_ALLOWED_SOURCES = {"igdb", "rawg", "steam", "steamspy", "hltb", "wikidata"}
RESOLVE_ALLOWED_SOURCES = {"igdb", "rawg", "steam", "hltb", "wikidata"}

# Base columns included in every provider output.
PROVIDER_BASE_COLS = ("RowId", "Name")


def provider_output_columns(df_columns: list[str], *, prefix: str, extra: tuple[str, ...] = ()) -> list[str]:
    """
    Build a stable provider output column list: base cols + provider-prefixed cols (+ extras).
    """
    cols = set(df_columns)
    out: list[str] = [c for c in PROVIDER_BASE_COLS if c in cols]
    out.extend([c for c in df_columns if c.startswith(prefix)])
    for c in extra:
        if c in cols and c not in out:
            out.append(c)
    return out


# Columns that pin identity per provider.
PINNED_ID_COLS = {
    "RAWG_ID",
    "IGDB_ID",
    "Steam_AppID",
    "HLTB_ID",
    "HLTB_Query",
    "Wikidata_QID",
}
