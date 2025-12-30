from __future__ import annotations

import logging
import re
from dataclasses import dataclass

import pandas as pd

from ...metrics.registry import MetricsRegistry
from ...utils import extract_year_hint, fuzzy_score
from ...utils.consistency import compute_provider_consensus, compute_year_consensus
from ...utils.utilities import IDENTITY_NOT_FOUND
from .import_diagnostics import fill_eval_tags


@dataclass(frozen=True)
class ResolveStats:
    attempted: int
    repinned: int
    unpinned: int
    kept: int
    wikidata_hint_added: int


def _diag_col(registry: MetricsRegistry, key: str) -> str | None:
    mapped = registry.diagnostic_column_for_key(key)
    if mapped is None:
        return None
    col, _typ = mapped
    return col


def _diag_get(row: pd.Series, registry: MetricsRegistry, key: str) -> str:
    col = _diag_col(registry, key)
    if not col:
        return ""
    return str(row.get(col, "") or "").strip()


def _diag_set(df: pd.DataFrame, idx: object, registry: MetricsRegistry, key: str, value: object) -> None:
    col = _diag_col(registry, key)
    if not col:
        return
    if col not in df.columns:
        df[col] = ""
    df.at[idx, col] = value


def auto_unpin_likely_wrong_provider_ids(
    df: pd.DataFrame, *, registry: MetricsRegistry
) -> tuple[pd.DataFrame, int, list[int]]:
    """
    Clear provider IDs when the row already indicates a strict-majority consensus that the
    provider is the outlier and likely wrong.

    This is intentionally conservative: it is meant to prevent "wrong pins", not to resolve
    ambiguity. It does not call provider APIs.
    """
    out = df.copy()
    changed = 0
    changed_idx: list[int] = []

    rules: list[tuple[str, str, list[str]]] = [
        (
            "steam",
            "Steam_AppID",
            [
                "diagnostics.steam.matched_name",
                "diagnostics.steam.match_score",
                "diagnostics.steam.matched_year",
                "diagnostics.steam.rejected_reason",
            ],
        ),
        (
            "rawg",
            "RAWG_ID",
            [
                "diagnostics.rawg.matched_name",
                "diagnostics.rawg.match_score",
                "diagnostics.rawg.matched_year",
            ],
        ),
        (
            "igdb",
            "IGDB_ID",
            [
                "diagnostics.igdb.matched_name",
                "diagnostics.igdb.match_score",
                "diagnostics.igdb.matched_year",
            ],
        ),
        (
            "hltb",
            "HLTB_ID",
            [
                "diagnostics.hltb.matched_name",
                "diagnostics.hltb.match_score",
                "diagnostics.hltb.matched_year",
                "diagnostics.hltb.matched_platforms",
            ],
        ),
    ]

    for idx, row in out.iterrows():
        rowid = str(row.get("RowId", "") or "").strip()
        if not rowid:
            continue
        tags = _diag_get(row, registry, "diagnostics.review.tags")
        if not tags:
            continue
        for prov, id_col, diag_cols in rules:
            id_val = str(row.get(id_col, "") or "").strip()
            if not id_val or id_val == IDENTITY_NOT_FOUND:
                continue
            if f"likely_wrong:{prov}" not in tags:
                continue
            if "provider_consensus:" not in tags:
                continue
            if f"provider_outlier:{prov}" not in tags:
                continue

            out.at[idx, id_col] = ""
            for key in diag_cols:
                _diag_set(out, idx, registry, key, "")

            existing = str(_diag_get(out.loc[idx], registry, "diagnostics.review.tags") or "").strip()
            if f"autounpinned:{prov}" not in existing:
                _diag_set(
                    out,
                    idx,
                    registry,
                    "diagnostics.review.tags",
                    (existing + f", autounpinned:{prov}").strip(", ").strip(),
                )
            _diag_set(out, idx, registry, "diagnostics.match.confidence", "LOW")
            changed += 1
            try:
                pos = out.index.get_loc(idx)
                if isinstance(pos, int):
                    changed_idx.append(pos)
            except Exception:
                pass

    return out, changed, changed_idx


def resolve_catalog_pins(
    df: pd.DataFrame,
    *,
    sources: set[str],
    clients: dict[str, object],
    retry_missing: bool,
    apply: bool,
    registry: MetricsRegistry,
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

    df = fill_eval_tags(df, sources=set(sources), clients=clients, registry=registry)

    def _majority_title_and_year(row: pd.Series) -> tuple[str, int | None] | None:
        title_keys = {
            "rawg": "diagnostics.rawg.matched_name",
            "igdb": "diagnostics.igdb.matched_name",
            "steam": "diagnostics.steam.matched_name",
            "hltb": "diagnostics.hltb.matched_name",
            "wikidata": "diagnostics.wikidata.matched_label",
        }
        year_keys = {
            "rawg": "diagnostics.rawg.matched_year",
            "igdb": "diagnostics.igdb.matched_year",
            "steam": "diagnostics.steam.matched_year",
            "hltb": "diagnostics.hltb.matched_year",
            "wikidata": "diagnostics.wikidata.matched_year",
        }

        titles: dict[str, str] = {}
        years: dict[str, int] = {}
        for p, key in title_keys.items():
            t = _diag_get(row, registry, key)
            if t:
                titles[p] = t
        for p, key in year_keys.items():
            y = _parse_int_year(_diag_get(row, registry, key))
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
            key = title_keys.get(p)
            title = _diag_get(row, registry, str(key or ""))
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
                return str((obj or {}).get("igdb.name") or "").strip()
        if provider == "steam" and "steam" in clients:
            sid = str(row.get("Steam_AppID", "") or "").strip()
            if sid.isdigit():
                details = clients["steam"].get_app_details(int(sid))  # type: ignore[attr-defined]
                return str((details or {}).get("name") or "").strip()
        if provider == "wikidata" and "wikidata" in clients:
            qid = str(row.get("Wikidata_QID", "") or "").strip()
            if qid:
                obj = clients["wikidata"].get_by_id(qid)  # type: ignore[attr-defined]
                return str((obj or {}).get("wikidata.label") or "").strip()
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
            enwiki = str((ent or {}).get("wikidata.enwiki_title") or "").strip()
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

    def _clear_provider_pin(row_idx: object, provider: str) -> None:
        if provider == "steam":
            df.at[row_idx, "Steam_AppID"] = ""
            for k in (
                "diagnostics.steam.matched_name",
                "diagnostics.steam.match_score",
                "diagnostics.steam.matched_year",
                "diagnostics.steam.rejected_reason",
            ):
                _diag_set(df, row_idx, registry, k, "")
        elif provider == "rawg":
            df.at[row_idx, "RAWG_ID"] = ""
            for k in (
                "diagnostics.rawg.matched_name",
                "diagnostics.rawg.match_score",
                "diagnostics.rawg.matched_year",
            ):
                _diag_set(df, row_idx, registry, k, "")
        elif provider == "igdb":
            df.at[row_idx, "IGDB_ID"] = ""
            for k in (
                "diagnostics.igdb.matched_name",
                "diagnostics.igdb.match_score",
                "diagnostics.igdb.matched_year",
            ):
                _diag_set(df, row_idx, registry, k, "")

    for idx, row in df.iterrows():
        if _is_yes(row.get("Disabled", "")):
            continue
        tags = _diag_get(row, registry, "diagnostics.review.tags")

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

        missing_wikidata_qid = "wikidata" in clients and not str(row.get("Wikidata_QID", "") or "").strip()
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
            if got and str(got.get("wikidata.qid", "") or "").strip():
                if apply:
                    df.at[idx, "Wikidata_QID"] = str(got.get("wikidata.qid") or "").strip()
                resolved_wikidata += 1
                if apply:
                    _diag_set(
                        df,
                        idx,
                        registry,
                        "diagnostics.review.tags",
                        _add_tag(
                            _diag_get(df.loc[idx], registry, "diagnostics.review.tags"),
                            "wikidata_hint",
                        ),
                    )

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
                        matched = str((details or {}).get("name") or search.get("name") or "").strip()
                        release = (details or {}).get("release_date", {}) or {}
                        m = re.search(r"\b(19\d{2}|20\d{2})\b", str(release.get("date", "") or ""))
                        y = m.group(1) if m else ""
                        score = fuzzy_score(majority_title or name, matched) if matched else 0
                        if score >= 90 or _year_close(y, majority_year):
                            if apply:
                                df.at[idx, "Steam_AppID"] = appid_str
                                _diag_set(
                                    df,
                                    idx,
                                    registry,
                                    "diagnostics.steam.matched_name",
                                    matched,
                                )
                                _diag_set(
                                    df,
                                    idx,
                                    registry,
                                    "diagnostics.steam.match_score",
                                    int(fuzzy_score(name, matched)) if matched else "",
                                )
                                _diag_set(df, idx, registry, "diagnostics.steam.matched_year", y)
                                _diag_set(df, idx, registry, "diagnostics.steam.rejected_reason", "")
                                _diag_set(
                                    df,
                                    idx,
                                    registry,
                                    "diagnostics.review.tags",
                                    _add_tag(
                                        _diag_get(df.loc[idx], registry, "diagnostics.review.tags"),
                                        "repinned_by_resolve:steam",
                                    ),
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
                            _diag_set(
                                df,
                                idx,
                                registry,
                                "diagnostics.rawg.matched_name",
                                matched,
                            )
                            _diag_set(
                                df,
                                idx,
                                registry,
                                "diagnostics.rawg.match_score",
                                int(fuzzy_score(name, matched)) if matched else "",
                            )
                            _diag_set(df, idx, registry, "diagnostics.rawg.matched_year", y)
                            _diag_set(
                                df,
                                idx,
                                registry,
                                "diagnostics.review.tags",
                                _add_tag(
                                    _diag_get(df.loc[idx], registry, "diagnostics.review.tags"),
                                    "repinned_by_resolve:rawg",
                                ),
                            )
                        repinned += 1
                        repin_ok = True
            elif prov == "igdb":
                igdb = clients["igdb"]
                logging.debug(f"[RESOLVE] Retry IGDB for '{name}' using '{retry_query}'")
                obj = igdb.search(retry_query, year_hint=retry_year)  # type: ignore[attr-defined]
                if obj and str(obj.get("igdb.id", "") or "").strip():
                    matched = str(obj.get("igdb.name") or "").strip()
                    y = str(obj.get("igdb.year") or "").strip()
                    score = fuzzy_score(majority_title or name, matched) if matched else 0
                    if score >= 90 or _year_close(y, majority_year):
                        if apply:
                            df.at[idx, "IGDB_ID"] = str(obj.get("igdb.id") or "").strip()
                            _diag_set(
                                df,
                                idx,
                                registry,
                                "diagnostics.igdb.matched_name",
                                matched,
                            )
                            _diag_set(
                                df,
                                idx,
                                registry,
                                "diagnostics.igdb.match_score",
                                int(fuzzy_score(name, matched)) if matched else "",
                            )
                            _diag_set(df, idx, registry, "diagnostics.igdb.matched_year", y)
                            _diag_set(
                                df,
                                idx,
                                registry,
                                "diagnostics.review.tags",
                                _add_tag(
                                    _diag_get(df.loc[idx], registry, "diagnostics.review.tags"),
                                    "repinned_by_resolve:igdb",
                                ),
                            )
                        repinned += 1
                        repin_ok = True

            if repin_ok:
                continue
            if reason == "fix_wrong_pin":
                logging.warning(
                    f"[RESOLVE] {'Unpinned' if apply else 'Would unpin'} likely-wrong {prov} for "
                    f"'{name}' (RowId={row.get('RowId', '')})"
                )
                if apply:
                    _clear_provider_pin(idx, prov)
                    _diag_set(
                        df,
                        idx,
                        registry,
                        "diagnostics.review.tags",
                        _add_tag(
                            _diag_get(df.loc[idx], registry, "diagnostics.review.tags"),
                            f"autounpinned:{prov}",
                        ),
                    )
                unpinned += 1
            else:
                kept += 1

    if apply and (attempted or repinned or resolved_wikidata or unpinned):
        df = fill_eval_tags(df, sources=set(sources), clients=clients, registry=registry)

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
