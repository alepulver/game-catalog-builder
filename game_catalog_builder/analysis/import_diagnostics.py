"""
Import diagnostics/tagging helpers.
"""

from __future__ import annotations

import re

import pandas as pd

from ..schema import DIAGNOSTIC_COLUMNS
from ..utils.consistency import (
    actionable_mismatch_tags,
    compute_provider_consensus,
    platform_outlier_tags,
    year_outlier_tags,
)
from ..utils.cross_refs import extract_steam_appid_from_rawg_stores
from ..utils.utilities import IDENTITY_NOT_FOUND, normalize_game_name


def platform_is_pc_like(platform_value: object) -> bool:
    p = str(platform_value or "").strip().lower()
    if not p:
        return True
    return any(x in p for x in ("pc", "windows", "steam", "linux", "mac", "osx"))


def fill_eval_tags(
    df: pd.DataFrame, *, sources: set[str] | None = None, clients: dict[str, object] | None = None
) -> pd.DataFrame:
    """
    Compute compact import diagnostics (`ReviewTags`, `MatchConfidence`) using both:
      - per-provider matching diagnostics columns (*_MatchScore, *_MatchedName, etc), and
      - optional provider payload lookups (via the passed `clients`) for high-signal checks.

    This function is pure with respect to provider caches: it only calls client getters and
    does not perform pin/unpin mutations.
    """
    from ..utils.utilities import ensure_columns

    out = df.copy()
    out = ensure_columns(out, {"ReviewTags": "", "MatchConfidence": ""})

    preserve_prefixes = (
        "autounpinned:",
        "repinned_by_resolve:",
        "wikidata_hint",
    )

    tags_list: list[str] = []
    confidence_list: list[str] = []

    include_rawg = sources is None or "rawg" in sources
    include_igdb = sources is None or "igdb" in sources
    include_steam = sources is None or "steam" in sources
    include_hltb = sources is None or "hltb" in sources

    def _is_yes(v: object) -> bool:
        return str(v or "").strip().upper() in {"YES", "Y", "TRUE", "1"}

    def _int_year(v: object) -> int | None:
        s = str(v or "").strip()
        if s.isdigit() and len(s) == 4:
            y = int(s)
            if 1900 <= y <= 2100:
                return y
        return None

    def _steam_year(details: object) -> int | None:
        if not isinstance(details, dict):
            return None
        date = str((details.get("release_date") or {}).get("date") or "")
        m = re.search(r"\b(19\d{2}|20\d{2})\b", date)
        if not m:
            return None
        try:
            return int(m.group(1))
        except ValueError:
            return None

    def _rawg_year(obj: object) -> int | None:
        if not isinstance(obj, dict):
            return None
        released = str(obj.get("released", "") or "").strip()
        if len(released) >= 4 and released[:4].isdigit():
            return int(released[:4])
        return None

    def _series_numbers(title: str) -> set[int]:
        tokens = normalize_game_name(title).split()
        out_set: set[int] = set()
        for i, t in enumerate(tokens):
            if not t.isdigit():
                continue
            # Ignore thousands-group patterns like "40,000" which normalize to "40 000".
            if i + 1 < len(tokens) and tokens[i + 1].isdigit() and tokens[i + 1] == "000":
                continue
            # Avoid leading-zero “brand” tokens like 007.
            if len(t) > 1 and t.startswith("0"):
                continue
            n = int(t)
            if n == 0:
                continue
            if 1900 <= n <= 2100:
                continue
            if 0 < n <= 50:
                out_set.add(n)
        return out_set

    def _platform_bucket(name: str) -> str | None:
        n = normalize_game_name(name)
        if any(x in n for x in ("pc", "windows", "mac", "osx", "linux")):
            return "pc"
        if "playstation" in n or n.startswith("ps"):
            return "playstation"
        if "xbox" in n:
            return "xbox"
        if any(x in n for x in ("nintendo", "switch", "wii")):
            return "nintendo"
        if any(x in n for x in ("ios", "android", "mobile")):
            return "mobile"
        return None

    def _platforms_from_csv_list(s: str) -> set[str]:
        out_set: set[str] = set()
        for part in [p.strip() for p in s.split(",") if p.strip()]:
            b = _platform_bucket(part)
            if b:
                out_set.add(b)
        return out_set

    def _platforms_from_rawg(obj: object) -> set[str]:
        if not isinstance(obj, dict):
            return set()
        out_set: set[str] = set()
        for it in obj.get("platforms", []) or []:
            if not isinstance(it, dict):
                continue
            pname = (it.get("platform") or {}).get("name")
            if not pname:
                continue
            b = _platform_bucket(str(pname))
            if b:
                out_set.add(b)
        return out_set

    def _platforms_from_hltb(obj: object) -> set[str]:
        if not isinstance(obj, dict):
            return set()
        return _platforms_from_csv_list(str(obj.get("HLTB_Platforms", "") or ""))

    def _genres_from_steam(details: object) -> set[str]:
        if not isinstance(details, dict):
            return set()
        out_set: set[str] = set()
        for g in details.get("genres", []) or []:
            if not isinstance(g, dict):
                continue
            name = str(g.get("description", "") or "").strip()
            if name:
                out_set.add(normalize_game_name(name))
        return {x for x in out_set if x}

    def _genres_from_rawg(obj: object) -> set[str]:
        if not isinstance(obj, dict):
            return set()
        out_set: set[str] = set()
        for g in obj.get("genres", []) or []:
            if not isinstance(g, dict):
                continue
            name = str(g.get("name", "") or "").strip()
            if name:
                out_set.add(normalize_game_name(name))
        return out_set

    def _genres_from_csv_list(s: str) -> set[str]:
        out_set: set[str] = set()
        for part in [p.strip() for p in str(s or "").split(",") if p.strip()]:
            out_set.add(normalize_game_name(part))
        return {x for x in out_set if x}

    def _platforms_from_steam(details: object) -> set[str]:
        if not isinstance(details, dict):
            return set()
        plats = details.get("platforms") or {}
        if not isinstance(plats, dict):
            return set()
        out_set: set[str] = set()
        if plats.get("windows") or plats.get("mac") or plats.get("linux"):
            out_set.add("pc")
        return out_set

    for idx, row in out.iterrows():
        tags: list[str] = []
        disabled = _is_yes(row.get("Disabled", ""))
        if disabled:
            tags.append("disabled")

        has_missing_provider = False
        has_medium_issue = False
        has_low_issue = False

        name = str(row.get("Name", "") or "").strip()
        steam_missing_expected = False

        if include_rawg:
            rawg_id = str(row.get("RAWG_ID", "") or "").strip()
            if rawg_id == IDENTITY_NOT_FOUND:
                tags.append("rawg_not_found")
            elif not rawg_id:
                tags.append("missing_rawg")
                has_missing_provider = True
            elif not str(row.get("RAWG_MatchedName", "") or "").strip():
                tags.append("rawg_id_unresolved")
                has_low_issue = True

        if include_igdb:
            igdb_id = str(row.get("IGDB_ID", "") or "").strip()
            if igdb_id == IDENTITY_NOT_FOUND:
                tags.append("igdb_not_found")
            elif not igdb_id:
                tags.append("missing_igdb")
                has_missing_provider = True
            elif not str(row.get("IGDB_MatchedName", "") or "").strip():
                tags.append("igdb_id_unresolved")
                has_low_issue = True

        if include_steam:
            steam_id = str(row.get("Steam_AppID", "") or "").strip()
            steam_expected = platform_is_pc_like(row.get("Platform", ""))
            if steam_id == IDENTITY_NOT_FOUND:
                tags.append("steam_not_found")
            elif not steam_id and steam_expected:
                steam_missing_expected = True
                rejected = str(row.get("Steam_RejectedReason", "") or "").strip()
                if rejected:
                    tags.append("steam_rejected")
                    tags.append(f"steam_rejected:{rejected}")
                    has_low_issue = True
            elif steam_id and not str(row.get("Steam_MatchedName", "") or "").strip():
                tags.append("steam_id_unresolved")
                has_low_issue = True

        if include_hltb:
            hltb_id = str(row.get("HLTB_ID", "") or "").strip()
            hltb_query = str(row.get("HLTB_Query", "") or "").strip()
            hltb_name = str(row.get("HLTB_MatchedName", "") or "").strip()
            if hltb_id == IDENTITY_NOT_FOUND or hltb_query == IDENTITY_NOT_FOUND:
                tags.append("hltb_not_found")
            elif not hltb_name:
                tags.append("missing_hltb")
                has_missing_provider = True

        # Cross-provider Steam AppID disagreement tags (high signal).
        steam_id = str(row.get("Steam_AppID", "") or "").strip()
        if steam_id and steam_id != IDENTITY_NOT_FOUND and clients:
            if include_igdb and "igdb" in clients:
                igdb_id = str(row.get("IGDB_ID", "") or "").strip()
                if igdb_id and igdb_id != IDENTITY_NOT_FOUND:
                    igdb_obj = clients["igdb"].get_by_id(igdb_id)  # type: ignore[attr-defined]
                    igdb_steam = str((igdb_obj or {}).get("IGDB_SteamAppID", "") or "").strip()
                    if igdb_steam and igdb_steam.isdigit() and igdb_steam != steam_id:
                        tags.append("steam_appid_disagree:igdb")
                        has_low_issue = True
            if include_rawg and "rawg" in clients:
                rawg_id = str(row.get("RAWG_ID", "") or "").strip()
                if rawg_id and rawg_id != IDENTITY_NOT_FOUND:
                    rawg_obj = clients["rawg"].get_by_id(rawg_id)  # type: ignore[attr-defined]
                    rawg_steam = extract_steam_appid_from_rawg_stores(rawg_obj)
                    if rawg_steam and rawg_steam.isdigit() and rawg_steam != steam_id:
                        tags.append("steam_appid_disagree:rawg")
                        has_low_issue = True

        for score_col, tag_prefix, enabled in (
            ("RAWG_MatchScore", "rawg_score", include_rawg),
            ("IGDB_MatchScore", "igdb_score", include_igdb),
            ("Steam_MatchScore", "steam_score", include_steam),
            ("HLTB_MatchScore", "hltb_score", include_hltb),
        ):
            if not enabled:
                continue
            s = str(row.get(score_col, "") or "").strip()
            if s.isdigit() and int(s) < 100:
                tags.append(f"{tag_prefix}:{s}")
                score = int(s)
                if score < 80:
                    has_low_issue = True
                elif score < 95:
                    has_medium_issue = True

        # High signal metadata checks (requires cached provider payloads).
        years: dict[str, int] = {}
        platforms: dict[str, set[str]] = {}
        genres: dict[str, set[str]] = {}
        igdb_payload: dict[str, object] | None = None

        if clients and isinstance(clients, dict):
            if include_rawg:
                rawg_id = str(row.get("RAWG_ID", "") or "").strip()
                if rawg_id and rawg_id != IDENTITY_NOT_FOUND:
                    rawg_client = clients.get("rawg")
                    rawg_obj = rawg_client.get_by_id(rawg_id) if rawg_client else None
                    y = _rawg_year(rawg_obj)
                    if y is not None:
                        years["rawg"] = y
                    platforms["rawg"] = _platforms_from_rawg(rawg_obj)
                    genres["rawg"] = _genres_from_rawg(rawg_obj)

            if include_igdb:
                igdb_id = str(row.get("IGDB_ID", "") or "").strip()
                if igdb_id and igdb_id != IDENTITY_NOT_FOUND:
                    igdb_client = clients.get("igdb")
                    igdb_obj = igdb_client.get_by_id(igdb_id) if igdb_client else None
                    if isinstance(igdb_obj, dict):
                        igdb_payload = igdb_obj
                        y = _int_year(igdb_obj.get("IGDB_Year", ""))
                        if y is not None:
                            years["igdb"] = y
                        plats = str(igdb_obj.get("IGDB_Platforms", "") or "")
                        platforms["igdb"] = _platforms_from_csv_list(plats)
                        genres["igdb"] = _genres_from_csv_list(
                            str(igdb_obj.get("IGDB_Genres", "") or "")
                        )

                if include_steam:
                    steam_id = str(row.get("Steam_AppID", "") or "").strip()
                    if steam_id and steam_id.isdigit() and steam_id != IDENTITY_NOT_FOUND:
                        steam_client = clients.get("steam")
                        details = (
                            steam_client.get_app_details(int(steam_id)) if steam_client else None
                        )
                        y = _steam_year(details)
                        if y is not None:
                            years["steam"] = y
                        platforms["steam"] = _platforms_from_steam(details)
                        genres["steam"] = _genres_from_steam(details)

            if include_hltb:
                hltb_id = str(row.get("HLTB_ID", "") or "").strip()
                if hltb_id and hltb_id != IDENTITY_NOT_FOUND:
                    hltb_client = clients.get("hltb")
                    hltb_obj = hltb_client.get_by_id(hltb_id) if hltb_client else None
                    if isinstance(hltb_obj, dict):
                        y = _int_year(hltb_obj.get("HLTB_ReleaseYear", ""))
                        if y is not None:
                            years["hltb"] = y
                        platforms["hltb"] = _platforms_from_hltb(hltb_obj)

        if steam_missing_expected:
            other_plats = set()
            for p, s in platforms.items():
                if p == "steam":
                    continue
                other_plats |= s or set()
            if other_plats and "pc" not in other_plats:
                tags.append("missing_steam_nonpc")
            else:
                tags.append("missing_steam")
                has_missing_provider = True

        year_tags = year_outlier_tags(years, max_diff=1, ignore_providers_for_consensus={"steam"})

        plat_tags = platform_outlier_tags(platforms)
        if plat_tags:
            tags.extend(plat_tags)
            has_low_issue = True

        # Edition/port suspicion: if Steam's year is an outlier but IGDB indicates the match is a
        # port/edition/alternate version, tag it as informational rather than a generic mismatch.
        steam_year_outlier = "year_outlier:steam" in year_tags
        if steam_year_outlier and igdb_payload:
            if any(
                str(igdb_payload.get(k) or "").strip()
                for k in (
                    "IGDB_ParentGame",
                    "IGDB_VersionParent",
                    "IGDB_DLCs",
                    "IGDB_Expansions",
                    "IGDB_Ports",
                )
            ):
                tags.append("edition_or_port_suspected")

        titles: dict[str, str] = {}
        if include_rawg:
            t = str(row.get("RAWG_MatchedName", "") or "").strip()
            if t:
                titles["rawg"] = t
        if include_igdb:
            t = str(row.get("IGDB_MatchedName", "") or "").strip()
            if t:
                titles["igdb"] = t
        if include_steam:
            t = str(row.get("Steam_MatchedName", "") or "").strip()
            if t:
                titles["steam"] = t
        if include_hltb:
            t = str(row.get("HLTB_MatchedName", "") or "").strip()
            if t:
                titles["hltb"] = t

        consensus = compute_provider_consensus(
            titles,
            years=years if years else None,
            title_score_threshold=90,
            year_tolerance=1,
            ignore_year_providers={"steam"},
            min_providers=2,
        )
        if consensus:
            tags.extend(consensus.tags())
            if not consensus.has_majority:
                has_low_issue = True
            elif consensus.outliers:
                has_medium_issue = True

        if year_tags:
            tags.extend(year_tags)
            # Steam-only year drift is common for ports/remasters; treat it as a weak signal
            # unless paired with other disagreements.
            only_steam_outlier = all(
                (t == "year_outlier:steam") for t in year_tags if t.startswith("year_outlier:")
            ) and any(t == "year_outlier:steam" for t in year_tags)
            if only_steam_outlier and not any(
                t in tags
                for t in (
                    "provider_outlier:steam",
                    "platform_outlier:steam",
                    "title_mismatch",
                    "steam_appid_disagree:igdb",
                    "steam_appid_disagree:rawg",
                    "store_type_not_game:dlc",
                    "store_type_not_game:demo",
                    "store_type_not_game:soundtrack",
                    "store_type_not_game:advertising",
                )
            ):
                has_medium_issue = True
            else:
                has_low_issue = True

        # Genre disagreements: use RAWG/IGDB (and optional Steam tags) as a high-signal check.
        if genres.get("rawg") and genres.get("igdb"):
            inter = genres["rawg"] & genres["igdb"]
            if not inter:
                tags.append("genre_disagree")
                has_medium_issue = True

        actionable = actionable_mismatch_tags(
            provider_consensus=consensus,
            years=years,
            year_tags=year_tags,
            platform_tags=plat_tags,
        )
        if actionable:
            tags.extend(actionable)
            has_low_issue = True

        if include_steam:
            steam_name = str(row.get("Steam_MatchedName", "") or "").strip()
            steam_id = str(row.get("Steam_AppID", "") or "").strip()
            if steam_id and steam_id != IDENTITY_NOT_FOUND and name and steam_name:
                q_nums = _series_numbers(name)
                s_nums = _series_numbers(steam_name)
                if q_nums != s_nums:
                    tags.append("steam_series_mismatch")
                    has_low_issue = True
                store_type = str(row.get("Steam_StoreType", "") or "").strip().lower()
                if store_type and store_type != "game":
                    tags.append(f"store_type_not_game:{store_type}")
                    has_low_issue = True

        if disabled:
            confidence = ""
        elif has_low_issue:
            confidence = "LOW"
        elif has_missing_provider or has_medium_issue:
            confidence = "MEDIUM"
        else:
            confidence = "HIGH"

        # Preserve a small set of stable, tool-emitted tags across recomputations.
        prev = ""
        try:
            prev = str(df.at[idx, "ReviewTags"] or "")
        except Exception:
            prev = ""
        if prev:
            for t in [x.strip() for x in prev.split(",") if x.strip()]:
                if t == "wikidata_hint" or t.startswith(preserve_prefixes):
                    if t not in tags:
                        tags.append(t)

        tags_list.append(", ".join(tags))
        confidence_list.append(confidence)

    out["ReviewTags"] = pd.Series(tags_list, index=out.index)
    out["MatchConfidence"] = pd.Series(confidence_list, index=out.index)
    if "NeedsReview" in out.columns:
        out = out.drop(columns=["NeedsReview"])
    # Ensure any missing diagnostic cols are present for downstream commands.
    from ..utils.utilities import ensure_columns as _ensure_cols

    out = _ensure_cols(out, {c: "" for c in DIAGNOSTIC_COLUMNS if c not in out.columns})
    return out
