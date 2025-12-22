from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .utilities import normalize_game_name


@dataclass(frozen=True)
class ReviewConfig:
    max_rows: int = 200


def _int_year(v: Any) -> int | None:
    s = str(v or "").strip()
    if len(s) == 4 and s.isdigit():
        y = int(s)
        if 1900 <= y <= 2100:
            return y
    return None


def _split_tags(s: Any) -> list[str]:
    raw = str(s or "").strip()
    if not raw:
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]


def _row_priority(tags: list[str], confidence: str) -> int:
    conf = (confidence or "").strip().upper()
    score = 0
    if conf == "LOW":
        score += 50
    elif conf == "MEDIUM":
        score += 20
    # Tag weights (keep small and interpretable).
    for t in tags:
        if t.startswith("likely_wrong:"):
            score += 40
        elif t.startswith("provider_outlier:"):
            score += 12
        elif t in {
            "year_disagree",
            "platform_disagree",
            "steam_appid_disagree:igdb",
            "steam_appid_disagree:rawg",
            "steam_rejected",
            "steam_dlc_like",
        }:
            score += 10
        elif t.startswith("year_outlier:") or t.startswith("platform_outlier:"):
            score += 6
        elif t in {"genre_disagree", "edition_disagree", "ambiguous_title_year"}:
            score += 4
    return score


def _steam_url(appid: str) -> str:
    s = str(appid or "").strip()
    if s.isdigit():
        return f"https://store.steampowered.com/app/{s}/"
    return ""


def _hltb_url(hltb_id: str) -> str:
    s = str(hltb_id or "").strip()
    if s.isdigit():
        return f"https://howlongtobeat.com/game/{s}"
    return ""


def build_review_csv(
    catalog_df: pd.DataFrame,
    *,
    enriched_df: pd.DataFrame | None = None,
    config: ReviewConfig | None = None,
) -> pd.DataFrame:
    """
    Build a focused review CSV from an imported catalog (diagnostics-enabled), optionally
    enriching it with selected fields from the merged enriched output.
    """
    cfg = config or ReviewConfig()
    df = catalog_df.copy()
    if "RowId" not in df.columns:
        raise ValueError("catalog_df missing RowId")

    # Merge some high-value enrichment fields (when available) to help manual review.
    if enriched_df is not None and "RowId" in enriched_df.columns:
        want = [
            "RowId",
            "Steam_Name",
            "Steam_URL",
            "Steam_Website",
            "Steam_ShortDescription",
            "Steam_Developers",
            "Steam_Publishers",
            "RAWG_Name",
            "RAWG_Website",
            "RAWG_DescriptionRaw",
            "RAWG_Developers",
            "RAWG_Publishers",
            "IGDB_Name",
            "IGDB_Summary",
            "IGDB_Websites",
            "IGDB_Developers",
            "IGDB_Publishers",
            "HLTB_Name",
            "Wikidata_Wikipedia",
            "Wikidata_WikipediaPage",
            "Wikidata_WikipediaSummary",
            "Wikidata_WikipediaThumbnail",
        ]
        cols = [c for c in want if c in enriched_df.columns]
        if cols:
            e = enriched_df[cols].copy()
            df = df.merge(e, on="RowId", how="left", suffixes=("", "_enriched"))

    # Compute helper columns.
    df["__tags"] = df.get("ReviewTags", "").apply(_split_tags)
    df["__conf"] = df.get("MatchConfidence", "").astype(str)
    df["__priority"] = [
        _row_priority(tags, conf) for tags, conf in zip(df["__tags"], df["__conf"], strict=False)
    ]
    if "Steam_AppID" in df.columns:
        df["Steam_URL"] = df["Steam_AppID"].astype(str).apply(_steam_url)
    else:
        df["Steam_URL"] = ""
    if "HLTB_ID" in df.columns:
        df["HLTB_URL"] = df["HLTB_ID"].astype(str).apply(_hltb_url)
    else:
        df["HLTB_URL"] = ""

    # Keep rows that are plausibly actionable.
    def _include_row(row: pd.Series) -> bool:
        if str(row.get("Disabled", "") or "").strip().upper() in {"YES", "Y", "TRUE", "1"}:
            return False
        conf = str(row.get("MatchConfidence", "") or "").strip().upper()
        if conf in {"LOW", "MEDIUM"}:
            return True
        tags = _split_tags(row.get("ReviewTags", ""))
        outlier_prefixes = (
            "likely_wrong:",
            "provider_outlier:",
            "year_outlier:",
            "platform_outlier:",
        )
        return any(
            t.startswith(outlier_prefixes)
            for t in tags
        )

    df = df[df.apply(_include_row, axis=1)].copy()
    if df.empty:
        return df

    # Trim extremely long Wikipedia summaries for CSV ergonomics.
    if "Wikidata_WikipediaSummary" in df.columns:
        df["Wikidata_WikipediaSummary"] = df["Wikidata_WikipediaSummary"].apply(
            lambda s: (str(s)[:300] + "…") if isinstance(s, str) and len(s) > 300 else s
        )

    # Provide a light canonical name suggestion: prefer provider consensus titles when present.
    def _suggested_title(row: pd.Series) -> str:
        for c in ("IGDB_MatchedName", "RAWG_MatchedName", "Steam_MatchedName", "HLTB_MatchedName"):
            t = str(row.get(c, "") or "").strip()
            if t and normalize_game_name(t) != normalize_game_name(str(row.get("Name", "") or "")):
                return t
        return ""

    df["SuggestedTitle"] = df.apply(_suggested_title, axis=1)

    # Column order.
    base_cols = [
        "RowId",
        "Name",
        "YearHint",
        "Platform",
        "MatchConfidence",
        "ReviewTags",
        "SuggestedTitle",
        "RAWG_ID",
        "RAWG_MatchedName",
        "RAWG_MatchedYear",
        "RAWG_MatchScore",
        "IGDB_ID",
        "IGDB_MatchedName",
        "IGDB_MatchedYear",
        "IGDB_MatchScore",
        "Steam_AppID",
        "Steam_MatchedName",
        "Steam_MatchedYear",
        "Steam_MatchScore",
        "Steam_RejectedReason",
        "Steam_URL",
        "HLTB_ID",
        "HLTB_Query",
        "HLTB_MatchedName",
        "HLTB_MatchedYear",
        "HLTB_MatchScore",
        "HLTB_MatchedPlatforms",
        "HLTB_URL",
        "Wikidata_QID",
        "Wikidata_MatchedLabel",
        "Wikidata_MatchedYear",
        "Wikidata_MatchScore",
    ]
    extra_cols = [
        "Steam_Name",
        "Steam_Developers",
        "Steam_Publishers",
        "RAWG_Name",
        "RAWG_Developers",
        "RAWG_Publishers",
        "IGDB_Name",
        "IGDB_Developers",
        "IGDB_Publishers",
        "HLTB_Name",
        "Wikidata_Wikipedia",
        "Wikidata_WikipediaPage",
        "Wikidata_WikipediaThumbnail",
        "Wikidata_WikipediaSummary",
    ]
    cols = [c for c in base_cols if c in df.columns] + [c for c in extra_cols if c in df.columns]
    cols = cols + [c for c in df.columns if c not in cols and not c.startswith("__")]
    df = df.sort_values(["__priority", "Name"], ascending=[False, True]).head(int(cfg.max_rows))
    return df[cols].reset_index(drop=True)
