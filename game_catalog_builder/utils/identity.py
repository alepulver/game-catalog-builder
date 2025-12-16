from __future__ import annotations

import pandas as pd

from .utilities import IDENTITY_NOT_FOUND, fuzzy_score


def merge_identity_user_fields(new: pd.DataFrame, prev: pd.DataFrame) -> pd.DataFrame:
    """
    Preserve user-editable identity fields when regenerating Games_Identity.csv.

    Rule: for each preserved column, keep the previous value when it's non-empty; otherwise use
    the newly generated value.
    """
    if new is None or new.empty:
        return new
    if prev is None or prev.empty:
        return new
    if "RowId" not in new.columns or "RowId" not in prev.columns:
        return new

    preserved_cols = ["RAWG_ID", "IGDB_ID", "Steam_AppID", "HLTB_Query"]
    prev2 = prev.copy()
    prev2["RowId"] = prev2["RowId"].astype(str).str.strip()
    new2 = new.copy()
    new2["RowId"] = new2["RowId"].astype(str).str.strip()

    merged = new2.merge(
        prev2[["RowId"] + [c for c in preserved_cols if c in prev2.columns]],
        on="RowId",
        how="left",
        suffixes=("", "__prev"),
    )
    for c in preserved_cols:
        prev_col = f"{c}__prev"
        if prev_col not in merged.columns:
            continue
        prev_vals = merged[prev_col].astype(str).str.strip()
        use_prev = prev_vals != ""
        if c not in merged.columns:
            merged[c] = ""
        merged.loc[use_prev, c] = prev_vals[use_prev]
        merged = merged.drop(columns=[prev_col])
    return merged


def generate_identity_map(
    merged: pd.DataFrame,
    validation: pd.DataFrame | None = None,
    *,
    key_prefix: str = "row",
) -> pd.DataFrame:
    """
    Generate a stage-1 identity mapping table from merged output (+ optional validation report).

    This is intentionally derived from already-fetched provider results, so it can be produced
    without a full two-stage refactor.
    """
    df = merged.copy()

    def col(name: str) -> pd.Series:
        return df[name] if name in df.columns else pd.Series([""] * len(df))

    original = col("Name").astype(str)

    rawg_name = col("RAWG_Name").astype(str)
    igdb_name = col("IGDB_Name").astype(str)
    steam_name = col("Steam_Name").astype(str)
    hltb_name = col("HLTB_Name").astype(str)

    def score_series(a: pd.Series, b: pd.Series) -> pd.Series:
        out: list[str] = []
        for aa, bb in zip(a.tolist(), b.tolist()):
            aa = str(aa or "").strip()
            bb = str(bb or "").strip()
            if not aa or not bb:
                out.append("")
            else:
                out.append(str(fuzzy_score(aa, bb)))
        return pd.Series(out)

    out = pd.DataFrame(
        {
            "RowId": col("RowId").astype(str).str.strip(),
            "OriginalName": original.str.strip(),
            "RAWG_ID": col("RAWG_ID").astype(str).str.strip(),
            "RAWG_MatchedName": rawg_name.str.strip(),
            "RAWG_MatchScore": score_series(original, rawg_name),
            "IGDB_ID": col("IGDB_ID").astype(str).str.strip(),
            "IGDB_MatchedName": igdb_name.str.strip(),
            "IGDB_MatchScore": score_series(original, igdb_name),
            "Steam_AppID": col("Steam_AppID").astype(str).str.strip(),
            "Steam_MatchedName": steam_name.str.strip(),
            "Steam_MatchScore": score_series(original, steam_name),
            "HLTB_Query": "",
            "HLTB_MatchedName": hltb_name.str.strip(),
            "HLTB_MatchScore": score_series(original, hltb_name),
        }
    )

    out["ReviewTags"] = ""

    # Bring in only enough validation to drive review tags (diagnostics stay in
    # Validation_Report.csv).
    v = validation.copy() if isinstance(validation, pd.DataFrame) and not validation.empty else None

    def _is_not_found(v: object) -> bool:
        return str(v or "").strip() == IDENTITY_NOT_FOUND

    def needs_review_row(r: pd.Series) -> bool:
        # Missing IDs are review-worthy.
        if not str(r.get("RAWG_ID", "")).strip() and not _is_not_found(r.get("RAWG_ID", "")):
            return True
        if not str(r.get("IGDB_ID", "")).strip() and not _is_not_found(r.get("IGDB_ID", "")):
            return True
        # Steam/HLTB are optional but still useful to review when missing.
        if not str(r.get("Steam_AppID", "")).strip() and not _is_not_found(
            r.get("Steam_AppID", "")
        ):
            return True
        if not str(r.get("HLTB_MatchedName", "")).strip() and not _is_not_found(
            r.get("HLTB_Query", "")
        ):
            return True
        # Low match scores.
        for k in ("RAWG_MatchScore", "IGDB_MatchScore", "Steam_MatchScore", "HLTB_MatchScore"):
            s = str(r.get(k, "")).strip()
            if s.isdigit() and int(s) < 100:
                return True
        # Validation flags.
        if v is not None:
            i = int(r.name)
            for flag in (
                "TitleMismatch",
                "YearDisagree_RAWG_IGDB",
                "PlatformDisagree",
                "SteamAppIDMismatch",
            ):
                if flag in v.columns and str(v.at[i, flag] or "").strip() == "YES":
                    return True
        return False

    out["NeedsReview"] = out.apply(needs_review_row, axis=1).map(lambda x: "YES" if x else "")

    # Build compact review tags.
    tags_all: list[str] = []
    for i, r in out.iterrows():
        tags: list[str] = []
        rawg_id = str(r.get("RAWG_ID", "")).strip()
        igdb_id = str(r.get("IGDB_ID", "")).strip()
        steam_id = str(r.get("Steam_AppID", "")).strip()
        hltb_query = str(r.get("HLTB_Query", "")).strip()

        if rawg_id == IDENTITY_NOT_FOUND:
            tags.append("rawg_not_found")
        elif not rawg_id:
            tags.append("missing_rawg")
        if igdb_id == IDENTITY_NOT_FOUND:
            tags.append("igdb_not_found")
        elif not igdb_id:
            tags.append("missing_igdb")
        if steam_id == IDENTITY_NOT_FOUND:
            tags.append("steam_not_found")
        elif not steam_id:
            tags.append("missing_steam")
        if hltb_query == IDENTITY_NOT_FOUND:
            tags.append("hltb_not_found")
        elif not str(r.get("HLTB_MatchedName", "")).strip():
            tags.append("missing_hltb")

        for k, tag in (
            ("RAWG_MatchScore", "rawg_score"),
            ("IGDB_MatchScore", "igdb_score"),
            ("Steam_MatchScore", "steam_score"),
            ("HLTB_MatchScore", "hltb_score"),
        ):
            s = str(r.get(k, "")).strip()
            if s.isdigit() and int(s) < 100:
                tags.append(f"{tag}:{s}")

        if v is not None:
            for flag, tag in (
                ("TitleMismatch", "title_mismatch"),
                ("YearDisagree_RAWG_IGDB", "year_mismatch"),
                ("PlatformDisagree", "platform_mismatch"),
                ("SteamAppIDMismatch", "steam_appid_mismatch"),
            ):
                if flag in v.columns and str(v.at[i, flag] or "").strip() == "YES":
                    tags.append(tag)

        tags_all.append(", ".join(tags))

    out["ReviewTags"] = pd.Series(tags_all)
    return out
