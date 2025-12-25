from __future__ import annotations

import logging
import re
from dataclasses import dataclass

import pandas as pd

from ..diagnostics.import_diagnostics import fill_eval_tags
from ..utils import extract_year_hint, fuzzy_score
from ..utils.consistency import compute_provider_consensus, compute_year_consensus


@dataclass(frozen=True)
class ResolveStats:
    attempted: int
    repinned: int
    unpinned: int
    kept: int
    wikidata_hint_added: int


def resolve_catalog_pins(
    df: pd.DataFrame,
    *,
    sources: set[str],
    clients: dict[str, object],
    retry_missing: bool,
    apply: bool,
) -> tuple[pd.DataFrame, ResolveStats]:
    """
    Third-pass resolution: optionally repin or unpin provider IDs based on diagnostics tags.

    - Does not mutate pins unless `apply=True`.
    - Uses provider-backed majority consensus (titles/years) and alias hints to retry missing
      or likely-wrong pins conservatively.
    """

    def _is_yes(v: object) -> bool:
        return str(v or "").strip().upper() in {"YES", "Y", "TRUE", "1"}

    def _parse_int_year(text: object) -> int | None:
        s = str(text or "").strip()
        if s.isdigit() and len(s) == 4:
            y = int(s)
            if 1900 <= y <= 2100:
                return y
        return None

    def _year_hint(row: pd.Series) -> int | None:
        for col in ("YearHint", "Year", "ReleaseYear", "Release_Year"):
            if col not in row.index:
                continue
            v = str(row.get(col, "") or "").strip()
            if v.isdigit() and len(v) == 4:
                y = int(v)
                if 1900 <= y <= 2100:
                    return y
        return extract_year_hint(str(row.get("Name", "") or ""))

    df = fill_eval_tags(df, sources=set(sources), clients=clients)

    def _majority_title_and_year(row: pd.Series) -> tuple[str, int | None] | None:
        title_cols = {
            "rawg": "RAWG_MatchedName",
            "igdb": "IGDB_MatchedName",
            "steam": "Steam_MatchedName",
            "hltb": "HLTB_MatchedName",
            "wikidata": "Wikidata_MatchedLabel",
        }
        year_cols = {
            "rawg": "RAWG_MatchedYear",
            "igdb": "IGDB_MatchedYear",
            "steam": "Steam_MatchedYear",
            "hltb": "HLTB_MatchedYear",
            "wikidata": "Wikidata_MatchedYear",
        }

        titles: dict[str, str] = {}
        years: dict[str, int] = {}
        for p, col in title_cols.items():
            t = str(row.get(col, "") or "").strip()
            if t:
                titles[p] = t
        for p, col in year_cols.items():
            y = _parse_int_year(row.get(col, ""))
            if y is not None:
                years[p] = y

        consensus = compute_provider_consensus(
            titles,
            years=years if years else None,
            title_score_threshold=90,
            year_tolerance=1,
            ignore_year_providers={"steam"},
            min_providers=2,
        )
        if not consensus or not consensus.has_majority or not consensus.majority:
            return None

        personal = str(row.get("Name", "") or "").strip()
        best_title = ""
        best_score = -1
        maj_years: dict[str, int] = {}
        for p in consensus.majority:
            col = title_cols.get(p)
            if col:
                title = str(row.get(col, "") or "").strip()
                if title:
                    sc = fuzzy_score(personal, title)
                    if sc > best_score:
                        best_title, best_score = title, sc
            y = years.get(p)
            if y is not None:
                maj_years[p] = y

        year_consensus = compute_year_consensus(maj_years) if maj_years else None
        year = year_consensus.value if year_consensus and year_consensus.has_majority else None
        return best_title, year

    def _provider_title_from_id(row: pd.Series, provider: str) -> str:
        if provider == "rawg" and "rawg" in clients:
            rid = str(row.get("RAWG_ID", "") or "").strip()
            if rid:
                obj = clients["rawg"].get_by_id(rid)  # type: ignore[attr-defined]
                return str((obj or {}).get("name") or "").strip()
        if provider == "igdb" and "igdb" in clients:
            iid = str(row.get("IGDB_ID", "") or "").strip()
            if iid:
                obj = clients["igdb"].get_by_id(iid)  # type: ignore[attr-defined]
                return str((obj or {}).get("IGDB_Name") or "").strip()
        if provider == "steam" and "steam" in clients:
            sid = str(row.get("Steam_AppID", "") or "").strip()
            if sid.isdigit():
                details = clients["steam"].get_app_details(int(sid))  # type: ignore[attr-defined]
                return str((details or {}).get("name") or "").strip()
        if provider == "wikidata" and "wikidata" in clients:
            qid = str(row.get("Wikidata_QID", "") or "").strip()
            if qid:
                obj = clients["wikidata"].get_by_id(qid)  # type: ignore[attr-defined]
                return str((obj or {}).get("Wikidata_Label") or "").strip()
        return ""

    def _pick_retry_query(
        row: pd.Series,
        majority_title: str,
        majority_year: int | None,
    ) -> tuple[str, int | None] | None:
        personal = str(row.get("Name", "") or "").strip()
        effective_year = majority_year if majority_year is not None else _year_hint(row)

        candidates: list[str] = []
        if majority_title:
            candidates.append(majority_title)

        for p in ("steam", "rawg", "igdb"):
            title = _provider_title_from_id(row, p)
            if title:
                candidates.append(title)

        qid = str(row.get("Wikidata_QID", "") or "").strip()
        if qid and "wikidata" in clients:
            wd = clients["wikidata"]
            aliases = wd.get_aliases(qid)  # type: ignore[attr-defined]
            candidates.extend(aliases[:10])
            ent = wd.get_by_id(qid)  # type: ignore[attr-defined]
            enwiki = str((ent or {}).get("Wikidata_EnwikiTitle") or "").strip()
            if enwiki:
                candidates.append(enwiki)

        igdb_id = str(row.get("IGDB_ID", "") or "").strip()
        if igdb_id and "igdb" in clients:
            alts = clients["igdb"].get_alternative_names(igdb_id)  # type: ignore[attr-defined]
            candidates.extend(alts[:10])

        target = majority_title or personal
        best = ""
        best_score = -1
        for c in candidates:
            s = str(c or "").strip()
            if not s:
                continue
            sc = fuzzy_score(target, s)
            if sc > best_score or (sc == best_score and len(s) < len(best)):
                best, best_score = s, sc
        if not best:
            return None
        return best, effective_year

    def _add_tag(existing: str, tag: str) -> str:
        s = str(existing or "").strip()
        if not s:
            return tag
        tags = [t.strip() for t in s.split(",") if t.strip()]
        if tag in tags:
            return s
        tags.append(tag)
        return ", ".join(tags)

    def _year_close(y: str, majority_year: int | None) -> bool:
        if majority_year is None:
            return True
        yy = _parse_int_year(y)
        if yy is None:
            return True
        return abs(yy - majority_year) <= 1

    attempted = 0
    repinned = 0
    kept = 0
    resolved_wikidata = 0
    unpinned = 0

    def _clear_provider_pin(row_idx: int, provider: str) -> None:
        if provider == "steam":
            for col in (
                "Steam_AppID",
                "Steam_MatchedName",
                "Steam_MatchScore",
                "Steam_MatchedYear",
                "Steam_RejectedReason",
                "Steam_StoreType",
            ):
                if col in df.columns:
                    df.at[row_idx, col] = ""
        elif provider == "rawg":
            for col in ("RAWG_ID", "RAWG_MatchedName", "RAWG_MatchScore", "RAWG_MatchedYear"):
                if col in df.columns:
                    df.at[row_idx, col] = ""
        elif provider == "igdb":
            for col in ("IGDB_ID", "IGDB_MatchedName", "IGDB_MatchScore", "IGDB_MatchedYear"):
                if col in df.columns:
                    df.at[row_idx, col] = ""

    for idx, row in df.iterrows():
        if _is_yes(row.get("Disabled", "")):
            continue
        tags = str(row.get("ReviewTags", "") or "")

        retry_targets: dict[str, str] = {}
        for prov, id_col in (
            ("steam", "Steam_AppID"),
            ("rawg", "RAWG_ID"),
            ("igdb", "IGDB_ID"),
        ):
            if prov not in clients:
                continue
            id_val = str(row.get(id_col, "") or "").strip()
            if (
                id_val
                and "provider_consensus:" in tags
                and f"provider_outlier:{prov}" in tags
                and f"likely_wrong:{prov}" in tags
            ):
                retry_targets[prov] = "fix_wrong_pin"
            elif f"autounpinned:{prov}" in tags and not id_val:
                retry_targets[prov] = "fill_after_unpin"
            elif retry_missing and not id_val:
                retry_targets[prov] = "fill_missing"

        missing_wikidata_qid = (
            "wikidata" in clients and not str(row.get("Wikidata_QID", "") or "").strip()
        )
        if not retry_targets and not missing_wikidata_qid:
            continue

        maj = _majority_title_and_year(row)
        if not maj:
            continue
        majority_title, majority_year = maj

        picked = _pick_retry_query(row, majority_title, majority_year)
        if not picked:
            continue
        retry_query, retry_year = picked

        if missing_wikidata_qid:
            steam_appid = str(row.get("Steam_AppID", "") or "").strip()
            igdb_id = str(row.get("IGDB_ID", "") or "").strip()
            got = clients["wikidata"].resolve_by_hints(  # type: ignore[attr-defined]
                steam_appid=steam_appid, igdb_id=igdb_id
            )
            if got and str(got.get("Wikidata_QID", "") or "").strip():
                if apply:
                    df.at[idx, "Wikidata_QID"] = str(got.get("Wikidata_QID") or "").strip()
                resolved_wikidata += 1
                if apply:
                    df.at[idx, "ReviewTags"] = _add_tag(df.at[idx, "ReviewTags"], "wikidata_hint")

        for prov, reason in retry_targets.items():
            attempted += 1
            name = str(row.get("Name", "") or "").strip()
            repin_ok = False
            if prov == "steam":
                steam = clients["steam"]
                logging.debug(f"[RESOLVE] Retry Steam for '{name}' using '{retry_query}'")
                search = steam.search_appid(retry_query, year_hint=retry_year)  # type: ignore[attr-defined]
                if search and search.get("id") is not None:
                    appid_str = str(search.get("id") or "").strip()
                    if appid_str.isdigit():
                        details = steam.get_app_details(int(appid_str))  # type: ignore[attr-defined]
                        matched = str(
                            (details or {}).get("name") or search.get("name") or ""
                        ).strip()
                        release = (details or {}).get("release_date", {}) or {}
                        m = re.search(
                            r"\b(19\d{2}|20\d{2})\b", str(release.get("date", "") or "")
                        )
                        y = m.group(1) if m else ""
                        score = fuzzy_score(majority_title or name, matched) if matched else 0
                        if score >= 90 or _year_close(y, majority_year):
                            if apply:
                                df.at[idx, "Steam_AppID"] = appid_str
                                df.at[idx, "Steam_MatchedName"] = matched
                                df.at[idx, "Steam_MatchScore"] = (
                                    str(fuzzy_score(name, matched)) if matched else ""
                                )
                                df.at[idx, "Steam_MatchedYear"] = y
                                df.at[idx, "Steam_StoreType"] = (
                                    str((details or {}).get("type") or "").strip().lower()
                                )
                                df.at[idx, "Steam_RejectedReason"] = ""
                                df.at[idx, "ReviewTags"] = _add_tag(
                                    df.at[idx, "ReviewTags"], "repinned_by_resolve:steam"
                                )
                            repinned += 1
                            repin_ok = True
            elif prov == "rawg":
                rawg = clients["rawg"]
                logging.debug(f"[RESOLVE] Retry RAWG for '{name}' using '{retry_query}'")
                obj = rawg.search(retry_query, year_hint=retry_year)  # type: ignore[attr-defined]
                if obj and obj.get("id") is not None:
                    matched = str(obj.get("name") or "").strip()
                    released = str(obj.get("released") or "").strip()
                    y = released[:4] if len(released) >= 4 else ""
                    score = fuzzy_score(majority_title or name, matched) if matched else 0
                    if score >= 90 or _year_close(y, majority_year):
                        if apply:
                            df.at[idx, "RAWG_ID"] = str(obj.get("id") or "").strip()
                            df.at[idx, "RAWG_MatchedName"] = matched
                            df.at[idx, "RAWG_MatchScore"] = (
                                str(fuzzy_score(name, matched)) if matched else ""
                            )
                            df.at[idx, "RAWG_MatchedYear"] = y
                            df.at[idx, "ReviewTags"] = _add_tag(
                                df.at[idx, "ReviewTags"], "repinned_by_resolve:rawg"
                            )
                        repinned += 1
                        repin_ok = True
            elif prov == "igdb":
                igdb = clients["igdb"]
                logging.debug(f"[RESOLVE] Retry IGDB for '{name}' using '{retry_query}'")
                obj = igdb.search(retry_query, year_hint=retry_year)  # type: ignore[attr-defined]
                if obj and str(obj.get("IGDB_ID", "") or "").strip():
                    matched = str(obj.get("IGDB_Name") or "").strip()
                    y = str(obj.get("IGDB_Year") or "").strip()
                    score = fuzzy_score(majority_title or name, matched) if matched else 0
                    if score >= 90 or _year_close(y, majority_year):
                        if apply:
                            df.at[idx, "IGDB_ID"] = str(obj.get("IGDB_ID") or "").strip()
                            df.at[idx, "IGDB_MatchedName"] = matched
                            df.at[idx, "IGDB_MatchScore"] = (
                                str(fuzzy_score(name, matched)) if matched else ""
                            )
                            df.at[idx, "IGDB_MatchedYear"] = y
                            df.at[idx, "ReviewTags"] = _add_tag(
                                df.at[idx, "ReviewTags"], "repinned_by_resolve:igdb"
                            )
                        repinned += 1
                        repin_ok = True

            if repin_ok:
                continue
            if reason == "fix_wrong_pin":
                logging.warning(
                    f"[RESOLVE] {'Unpinned' if apply else 'Would unpin'} likely-wrong {prov} for "
                    f"'{name}' (RowId={row.get('RowId','')})"
                )
                if apply:
                    _clear_provider_pin(idx, prov)
                    df.at[idx, "ReviewTags"] = _add_tag(df.at[idx, "ReviewTags"], f"autounpinned:{prov}")
                unpinned += 1
            else:
                kept += 1

    if apply and (attempted or repinned or resolved_wikidata or unpinned):
        df = fill_eval_tags(df, sources=set(sources), clients=clients)

    return (
        df,
        ResolveStats(
            attempted=attempted,
            repinned=repinned,
            unpinned=unpinned,
            kept=kept,
            wikidata_hint_added=resolved_wikidata,
        ),
    )

