from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import pandas as pd

from ..metrics.registry import MetricsRegistry, default_metrics_registry_path, load_metrics_registry
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
    registry: MetricsRegistry | None = None,
) -> pd.DataFrame:
    """
    Build a focused review CSV from an imported catalog (diagnostics-enabled), optionally
    enriching it with selected fields from the merged enriched output.
    """
    reg = registry or load_metrics_registry(default_metrics_registry_path())
    cfg = config or ReviewConfig()
    df = catalog_df.copy()
    if "RowId" not in df.columns:
        raise ValueError("catalog_df missing RowId")

    def _diag_col(key: str, *, fallback: str) -> str:
        mapped = reg.diagnostic_column_for_key(key)
        if mapped is None:
            return fallback
        col, _typ = mapped
        return col

    def _metric_col(key: str) -> str | None:
        mapped = reg.column_for_key(key)
        if mapped is None:
            return None
        col, _typ = mapped
        return col

    tags_col = _diag_col("diagnostics.review.tags", fallback="ReviewTags")
    conf_col = _diag_col("diagnostics.match.confidence", fallback="MatchConfidence")

    # Merge some high-value enrichment fields (when available) to help manual review.
    if enriched_df is not None and "RowId" in enriched_df.columns:
        want_keys = [
            "steam.name",
            "steam.url",
            "steam.website",
            "steam.short_description",
            "steam.developers",
            "steam.publishers",
            "rawg.name",
            "rawg.website",
            "rawg.description_raw",
            "rawg.developers",
            "rawg.publishers",
            "igdb.name",
            "igdb.summary",
            "igdb.websites",
            "igdb.developers",
            "igdb.publishers",
            "hltb.name",
            "wikidata.wikipedia",
            "wikipedia.page_url",
            "wikipedia.summary",
            "wikipedia.thumbnail",
        ]
        cols = ["RowId"] + [c for c in (_metric_col(k) for k in want_keys) if c]
        cols = [c for c in cols if c in enriched_df.columns]
        if cols:
            e = enriched_df[cols].copy()
            df = df.merge(e, on="RowId", how="left", suffixes=("", "_enriched"))

    # Compute helper columns.
    review_tags = (
        cast(pd.Series, df[tags_col])
        if tags_col in df.columns
        else pd.Series([""] * len(df), index=df.index, dtype=str)
    )
    df["__tags"] = review_tags.map(_split_tags)
    match_conf = (
        cast(pd.Series, df[conf_col])
        if conf_col in df.columns
        else pd.Series([""] * len(df), index=df.index, dtype=str)
    )
    df["__conf"] = match_conf.astype(str)
    df["__priority"] = [_row_priority(tags, conf) for tags, conf in zip(df["__tags"], df["__conf"])]
    # Convenience URLs from pinned IDs (don't depend on enrichment fields).
    steam_url_col = _metric_col("steam.url") or "Steam_URL"
    if steam_url_col not in df.columns:
        df[steam_url_col] = ""
    if "Steam_AppID" in df.columns:
        mask = df[steam_url_col].astype(str).str.strip().eq("")
        df.loc[mask, steam_url_col] = df.loc[mask, "Steam_AppID"].astype(str).apply(_steam_url)
    hltb_url_col = "HLTB_URL"
    if hltb_url_col not in df.columns:
        df[hltb_url_col] = ""
    if "HLTB_ID" in df.columns:
        mask = df[hltb_url_col].astype(str).str.strip().eq("")
        df.loc[mask, hltb_url_col] = df.loc[mask, "HLTB_ID"].astype(str).apply(_hltb_url)

    # Keep rows that are plausibly actionable.
    def _include_row(row: pd.Series) -> bool:
        if str(row.get("Disabled", "") or "").strip().upper() in {"YES", "Y", "TRUE", "1"}:
            return False
        conf = str(row.get(conf_col, "") or "").strip().upper()
        if conf in {"LOW", "MEDIUM"}:
            return True
        tags = _split_tags(row.get(tags_col, ""))
        outlier_prefixes = (
            "likely_wrong:",
            "provider_outlier:",
            "year_outlier:",
            "platform_outlier:",
        )
        return any(t.startswith(outlier_prefixes) for t in tags)

    df = cast(pd.DataFrame, df.loc[df.apply(_include_row, axis=1)].copy())
    if df.empty:
        return df

    # Trim extremely long Wikipedia summaries for CSV ergonomics.
    if "Wikidata_WikipediaSummary" in df.columns:
        summary = cast(pd.Series, df["Wikidata_WikipediaSummary"])
        df["Wikidata_WikipediaSummary"] = summary.map(
            lambda s: (str(s)[:300] + "…") if isinstance(s, str) and len(s) > 300 else s
        )

    # Provide a light canonical name suggestion: prefer provider consensus titles when present.
    def _suggested_title(row: pd.Series) -> str:
        for k in (
            "diagnostics.igdb.matched_name",
            "diagnostics.rawg.matched_name",
            "diagnostics.steam.matched_name",
            "diagnostics.hltb.matched_name",
        ):
            mapped = reg.diagnostic_column_for_key(k)
            if mapped is None:
                continue
            col, _typ = mapped
            t = str(row.get(col, "") or "").strip()
            if t and normalize_game_name(t) != normalize_game_name(str(row.get("Name", "") or "")):
                return t
        return ""

    df["SuggestedTitle"] = df.apply(_suggested_title, axis=1)

    # Column order.
    base_cols: list[str] = [
        "RowId",
        "Name",
        "YearHint",
        "Platform",
        conf_col,
        tags_col,
        "SuggestedTitle",
        "RAWG_ID",
        "IGDB_ID",
        "Steam_AppID",
        "HLTB_ID",
        "HLTB_Query",
        "Wikidata_QID",
    ]
    for k in (
        "diagnostics.rawg.matched_name",
        "diagnostics.rawg.matched_year",
        "diagnostics.rawg.match_score",
        "diagnostics.igdb.matched_name",
        "diagnostics.igdb.matched_year",
        "diagnostics.igdb.match_score",
        "diagnostics.steam.matched_name",
        "diagnostics.steam.matched_year",
        "diagnostics.steam.match_score",
        "diagnostics.steam.rejected_reason",
        "diagnostics.hltb.matched_name",
        "diagnostics.hltb.matched_year",
        "diagnostics.hltb.match_score",
        "diagnostics.hltb.matched_platforms",
        "diagnostics.wikidata.matched_label",
        "diagnostics.wikidata.matched_year",
        "diagnostics.wikidata.match_score",
    ):
        mapped = reg.diagnostic_column_for_key(k)
        if mapped is None:
            continue
        col, _typ = mapped
        base_cols.append(col)
    base_cols.extend([steam_url_col, hltb_url_col])

    extra_cols: list[str] = []
    for k in (
        "steam.name",
        "steam.website",
        "steam.short_description",
        "steam.developers",
        "steam.publishers",
        "rawg.name",
        "rawg.website",
        "rawg.description_raw",
        "rawg.developers",
        "rawg.publishers",
        "igdb.name",
        "igdb.summary",
        "igdb.websites",
        "igdb.developers",
        "igdb.publishers",
        "hltb.name",
        "wikidata.wikipedia",
        "wikipedia.page_url",
        "wikipedia.thumbnail",
        "wikipedia.summary",
    ):
        col = _metric_col(k)
        if col:
            extra_cols.append(col)
    cols = [c for c in base_cols if c in df.columns] + [c for c in extra_cols if c in df.columns]
    cols = cols + [c for c in df.columns if c not in cols and not c.startswith("__")]
    df = df.sort_values(by=["__priority", "Name"], ascending=[False, True]).head(int(cfg.max_rows))
    out = df[cols].reset_index(drop=True)
    return cast(pd.DataFrame, out)
