from __future__ import annotations

from typing import Any

# -----------------------------------------------------------------------------
# CSV schema / column sets
# -----------------------------------------------------------------------------

# Default public columns (for merges). These are the provider/public columns the project
# writes into provider outputs and the merged enriched CSV. Keep this centralized so we
# don't drift across commands.
PUBLIC_DEFAULT_COLS: dict[str, Any] = {
    # Stable input row key
    "RowId": "",
    # RAWG
    "RAWG_ID": "",
    "RAWG_Name": "",
    "RAWG_Released": "",
    "RAWG_Year": "",
    "RAWG_Website": "",
    "RAWG_DescriptionRaw": "",
    "RAWG_Genre": "",
    "RAWG_Genre2": "",
    "RAWG_Genres": "",
    "RAWG_Platforms": "",
    "RAWG_Tags": "",
    "RAWG_ESRB": "",
    "RAWG_Rating": "",
    "Score_RAWG_100": "",
    "RAWG_RatingsCount": "",
    "RAWG_Metacritic": "",
    "RAWG_Developers": "",
    "RAWG_Publishers": "",
    # IGDB
    "IGDB_ID": "",
    "IGDB_Name": "",
    "IGDB_Year": "",
    "IGDB_Summary": "",
    "IGDB_Websites": "",
    "IGDB_Platforms": "",
    "IGDB_Genres": "",
    "IGDB_Themes": "",
    "IGDB_GameModes": "",
    "IGDB_Perspectives": "",
    "IGDB_Franchise": "",
    "IGDB_Engine": "",
    "IGDB_ParentGame": "",
    "IGDB_VersionParent": "",
    "IGDB_DLCs": "",
    "IGDB_Expansions": "",
    "IGDB_Ports": "",
    "IGDB_SteamAppID": "",
    "IGDB_Developers": "",
    "IGDB_Publishers": "",
    "IGDB_Rating": "",
    "IGDB_RatingCount": "",
    "Score_IGDB_100": "",
    "IGDB_AggregatedRating": "",
    "IGDB_AggregatedRatingCount": "",
    "Score_IGDBCritic_100": "",
    # Steam
    "Steam_AppID": "",
    "Steam_Name": "",
    "Steam_URL": "",
    "Steam_Website": "",
    "Steam_ShortDescription": "",
    "Steam_StoreType": "",
    "Steam_ReleaseYear": "",
    "Steam_Platforms": "",
    "Steam_Tags": "",
    "Steam_ReviewCount": "",
    "Steam_Price": "",
    "Steam_Categories": "",
    "Steam_Metacritic": "",
    "Steam_Developers": "",
    "Steam_Publishers": "",
    # SteamSpy
    "SteamSpy_Owners": "",
    "SteamSpy_CCU": "",
    "SteamSpy_PlaytimeAvg": "",
    "SteamSpy_PlaytimeAvg2Weeks": "",
    "SteamSpy_PlaytimeMedian2Weeks": "",
    "SteamSpy_Positive": "",
    "SteamSpy_Negative": "",
    "Score_SteamSpy_100": "",
    # HLTB
    "HLTB_Name": "",
    "HLTB_Main": "",
    "HLTB_Extra": "",
    "HLTB_Completionist": "",
    "Score_HLTB_100": "",
    # Wikidata
    "Wikidata_QID": "",
    "Wikidata_Label": "",
    "Wikidata_Description": "",
    "Wikidata_ReleaseYear": "",
    "Wikidata_Developers": "",
    "Wikidata_Publishers": "",
    "Wikidata_Platforms": "",
    "Wikidata_Series": "",
    "Wikidata_Genres": "",
    "Wikidata_Wikipedia": "",
    # Wikipedia signals (official APIs, derived from Wikidata enwiki title)
    "Wikidata_WikipediaPage": "",
    "Wikidata_WikipediaSummary": "",
    "Wikidata_WikipediaThumbnail": "",
    "Wikidata_Pageviews30d": "",
    "Wikidata_Pageviews90d": "",
    "Wikidata_Pageviews365d": "",
    "Wikidata_PageviewsFirst30d": "",
    "Wikidata_PageviewsFirst90d": "",
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


def provider_output_columns(
    df_columns: list[str], *, prefix: str, extra: tuple[str, ...] = ()
) -> list[str]:
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

# Import diagnostics/evaluation columns (kept in Games_Catalog.csv when diagnostics enabled).
EVAL_COLUMNS = [
    "RAWG_MatchedName",
    "RAWG_MatchScore",
    "RAWG_MatchedYear",
    "IGDB_MatchedName",
    "IGDB_MatchScore",
    "IGDB_MatchedYear",
    "Steam_MatchedName",
    "Steam_MatchScore",
    "Steam_MatchedYear",
    "Steam_RejectedReason",
    "Steam_StoreType",
    "HLTB_MatchedName",
    "HLTB_MatchScore",
    "HLTB_MatchedYear",
    "HLTB_MatchedPlatforms",
    "Wikidata_MatchedLabel",
    "Wikidata_MatchScore",
    "Wikidata_MatchedYear",
    "ReviewTags",
    "MatchConfidence",
    # Legacy column kept only so we can drop it from older CSVs.
    "NeedsReview",
]

DIAGNOSTIC_COLUMNS = [c for c in EVAL_COLUMNS if c != "NeedsReview"]
