from __future__ import annotations

import pandas as pd

from .utilities import fuzzy_score


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
    df.insert(0, "InputRowKey", [f"{key_prefix}:{i+1:05d}" for i in range(len(df))])

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
            "InputRowKey": df["InputRowKey"].astype(str),
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
            "HLTB_ID": "",  # placeholder (HLTB id may not be stable/available)
            "HLTB_MatchedName": hltb_name.str.strip(),
            "HLTB_MatchScore": score_series(original, hltb_name),
        }
    )

    # Bring in canonical suggestions / validation flags if provided.
    if isinstance(validation, pd.DataFrame) and not validation.empty:
        v = validation.copy()
        # Align by row order (same as merged report generation).
        for c in [
            "MissingProviders",
            "TitleMismatch",
            "YearDisagree_RAWG_IGDB",
            "PlatformDisagree",
            "SteamAppIDMismatch",
            "ReviewTitle",
            "ReviewTitleReason",
            "SuggestedRenamePersonalName",
            "SuggestedCanonicalTitle",
            "SuggestedCanonicalSource",
            "SuggestedCulprit",
        ]:
            if c in v.columns:
                out[c] = v[c].astype(str)

    def needs_review_row(r: pd.Series) -> bool:
        # Missing IDs are review-worthy.
        if not str(r.get("RAWG_ID", "")).strip():
            return True
        if not str(r.get("IGDB_ID", "")).strip():
            return True
        # Steam/HLTB are optional but still useful to review when missing.
        if not str(r.get("Steam_AppID", "")).strip():
            return True
        if not str(r.get("HLTB_MatchedName", "")).strip():
            return True
        # Low match scores.
        for k in ("RAWG_MatchScore", "IGDB_MatchScore", "Steam_MatchScore", "HLTB_MatchScore"):
            s = str(r.get(k, "")).strip()
            if s.isdigit() and int(s) < 100:
                return True
        # Validation flags.
        for flag in ("TitleMismatch", "YearDisagree_RAWG_IGDB", "PlatformDisagree", "SteamAppIDMismatch", "ReviewTitle"):
            if str(r.get(flag, "")).strip() == "YES":
                return True
        return False

    out["NeedsReview"] = out.apply(needs_review_row, axis=1).map(lambda x: "YES" if x else "")
    return out

