from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from typing import Any

import requests

from ..config import MATCHING, REQUEST, RETRY, WIKIDATA
from ..utils.utilities import (
    CacheIOTracker,
    RateLimiter,
    fuzzy_score,
    iter_chunks,
    pick_best_match,
    with_retries,
)
from ..config import CACHE

WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
USER_AGENT = "game-catalog-builder/1.0 (contact: alepulver@protonmail.com)"

# External identifier properties (Wikidata wdt:Pxxxx).
# Keep these centralized so we can adjust if needed.
WIKIDATA_PROP_STEAM_APPID = "P1733"
WIKIDATA_PROP_IGDB_ID = "P5794"


class WikidataClient:
    """
    Wikidata enrichment via MediaWiki API.

    Cache format:
      - by_query: query_key -> list[search result dict] (from wbsearchentities)
      - by_hint: hint_key -> qid (external-id based resolver, e.g. Steam AppID)
      - by_id: qid -> raw wbgetentities entity dict (English labels/claims/sitelinks)
      - by_id_negative: list[str] of qids that failed
    """

    def __init__(self, cache_path: str | Path, min_interval_s: float = WIKIDATA.min_interval_s):
        self.cache_path = Path(cache_path)
        # Use a persistent session to avoid paying TLS handshake + connection setup cost on every
        # request. This matters a lot for label fetches during warm-cache import runs.
        self._session = requests.Session()
        self.stats: dict[str, int] = {
            "by_query_hit": 0,
            "by_query_fetch": 0,
            "by_query_negative_hit": 0,
            "by_query_negative_fetch": 0,
            "by_hint_hit": 0,
            "by_hint_fetch": 0,
            "by_hint_negative_hit": 0,
            "by_hint_negative_fetch": 0,
            "by_id_hit": 0,
            "by_id_fetch": 0,
            "by_id_negative_hit": 0,
            "by_id_negative_fetch": 0,
            # HTTP request counters (attempts, including retries).
            "http_sparql": 0,
            "http_wbsearchentities": 0,
            "http_wbgetentities": 0,
            "http_wbgetentities_labels": 0,
            # Label cache behavior.
            "labels_hit": 0,
            "labels_fetch": 0,
            # Cache IO.
            "cache_load_count": 0,
            "cache_load_ms": 0,
            "cache_save_count": 0,
            "cache_save_ms": 0,
        }
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)
        self._by_query: dict[str, list[dict[str, Any]]] = {}
        self._by_hint: dict[str, str] = {}
        self._by_id: dict[str, dict[str, Any]] = {}
        self._labels: dict[str, str] = {}
        # If a run is offline and label fetching fails, stop attempting further label HTTP calls.
        # This keeps cache-only runs fast and avoids spending minutes retrying the same endpoint.
        self._labels_fetch_disabled = False
        self._by_id_negative: set[str] = set()
        self._cache_io = CacheIOTracker(self.stats, min_interval_s=CACHE.save_min_interval_large_s)
        self._load_cache(self._cache_io.load_json(self.cache_path))

    def _load_cache(self, raw: Any) -> None:
        if not isinstance(raw, dict) or not raw:
            return
        if isinstance(raw.get("by_query"), dict):
            self._by_query = {
                str(k): [it for it in v if isinstance(it, dict)]
                for k, v in raw["by_query"].items()
                if isinstance(v, list)
            }
        if isinstance(raw.get("by_hint"), dict):
            self._by_hint = {str(k): str(v) for k, v in raw["by_hint"].items() if str(k).strip()}
        if isinstance(raw.get("by_id"), dict):
            self._by_id = {str(k): v for k, v in raw["by_id"].items() if isinstance(v, dict)}
        if isinstance(raw.get("labels"), dict):
            self._labels = {
                str(k): str(v).strip()
                for k, v in raw["labels"].items()
                if str(k).strip() and str(v).strip()
            }
        if isinstance(raw.get("by_id_negative"), list):
            self._by_id_negative = {str(x) for x in raw["by_id_negative"] if str(x).strip()}

    def _save_cache(self) -> None:
        self._cache_io.save_json(
            {
                "by_query": self._by_query,
                "by_hint": self._by_hint,
                "by_id": self._by_id,
                "labels": self._labels,
                "by_id_negative": sorted(self._by_id_negative),
            },
            self.cache_path,
        )

    def _sparql_select_qids(self, *, prop: str, value: str) -> list[str]:
        """
        Resolve Wikidata entity ids (QIDs) via an external identifier property.

        Example: prop="P1733", value="620" -> the entity whose Steam AppID is 620.
        """
        p = str(prop or "").strip()
        v = str(value or "").strip()
        if not p or not v:
            return []

        # Use a minimal query and then validate candidates via get_by_id() guards.
        query = f"""
        SELECT ?item WHERE {{
          ?item wdt:{p} "{v}" .
        }}
        LIMIT 10
        """.strip()

        def _request():
            self.ratelimiter.wait()
            self.stats["http_sparql"] += 1
            r = self._session.get(
                WIKIDATA_SPARQL_URL,
                params={"format": "json", "query": query},
                timeout=REQUEST.timeout_s,
                headers={"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"},
            )
            r.raise_for_status()
            return r.json()

        data = with_retries(
            _request,
            retries=RETRY.retries,
            on_fail_return=None,
            context=f"Wikidata SPARQL prop={p} value={v}",
            retry_stats=self.stats,
        )
        if not isinstance(data, dict):
            # If a network failure occurred, abort the operation instead of silently turning
            # this into "not found" results.
            if int(self.stats.get("network_failures", 0) or 0) > 0:
                raise RuntimeError(
                    "Wikidata request failed due to network issues (SPARQL). "
                    "Enable internet access or rerun with a warm cache."
                )
            return []
        results = (data.get("results") or {}).get("bindings") or []
        if not isinstance(results, list):
            return []
        out: list[str] = []
        for it in results:
            if not isinstance(it, dict):
                continue
            item = it.get("item") or {}
            if not isinstance(item, dict):
                continue
            uri = str(item.get("value") or "").strip()
            m = re.search(r"/entity/(Q\d+)$", uri)
            if m:
                out.append(m.group(1))
        # Dedup while preserving order.
        seen: set[str] = set()
        return [q for q in out if not (q in seen or seen.add(q))]

    def resolve_by_hints(
        self,
        *,
        steam_appid: str | None = None,
        igdb_id: str | None = None,
    ) -> dict[str, str] | None:
        """
        Resolve a Wikidata entity using provider-backed identifiers (no free-text search).

        This is intentionally conservative: it validates each candidate via get_by_id() and
        will return None if Wikidata does not clearly identify a video game entity.
        """

        def _try(prop: str, value: str) -> dict[str, str] | None:
            hint_key = f"hint:{prop}:{value}"
            cached = self._by_hint.get(hint_key)
            if cached is not None:
                qid = str(cached).strip()
                if not qid:
                    self.stats["by_hint_negative_hit"] += 1
                    return None
                self.stats["by_hint_hit"] += 1
                out = self.get_by_id(qid)
                return out

            qids = self._sparql_select_qids(prop=prop, value=value)
            for q in qids:
                got = self.get_by_id(q)
                if got:
                    self._by_hint[hint_key] = q
                    self._save_cache()
                    self.stats["by_hint_fetch"] += 1
                    return got
            # Negative-cache the hint lookup.
            self._by_hint[hint_key] = ""
            self._save_cache()
            self.stats["by_hint_negative_fetch"] += 1
            return None

        if steam_appid:
            got = _try(WIKIDATA_PROP_STEAM_APPID, str(steam_appid).strip())
            if got:
                return got
        if igdb_id:
            got = _try(WIKIDATA_PROP_IGDB_ID, str(igdb_id).strip())
            if got:
                return got
        return None

    def _search(self, query: str) -> list[dict[str, Any]]:
        q = str(query or "").strip()
        if not q:
            return []
        query_key = f"lang:en|search:{q}"
        cached = self._by_query.get(query_key)
        if cached is not None:
            if cached:
                self.stats["by_query_hit"] += 1
            else:
                self.stats["by_query_negative_hit"] += 1
            return cached

        def _request():
            self.ratelimiter.wait()
            self.stats["http_wbsearchentities"] += 1
            r = self._session.get(
                WIKIDATA_API_URL,
                params={
                    "action": "wbsearchentities",
                    "search": q,
                    "language": "en",
                    "limit": WIKIDATA.search_limit,
                    "format": "json",
                },
                timeout=REQUEST.timeout_s,
                headers={"User-Agent": USER_AGENT},
            )
            r.raise_for_status()
            return r.json()

        data = with_retries(
            _request,
            retries=RETRY.retries,
            on_fail_return=None,
            context="Wikidata search",
            retry_stats=self.stats,
        )
        if data is None:
            logging.warning(
                f"Wikidata search request failed for '{q}' (no response); not caching as not-found."
            )
            if int(self.stats.get("network_failures", 0) or 0) > 0:
                raise RuntimeError(
                    "Wikidata request failed due to network issues (search). "
                    "Enable internet access or rerun with a warm cache."
                )
            return []
        if not isinstance(data, dict):
            self._by_query[query_key] = []
            self._save_cache()
            self.stats["by_query_negative_fetch"] += 1
            return []
        items = [it for it in (data.get("search") or []) if isinstance(it, dict)]
        self._by_query[query_key] = items
        self._save_cache()
        self.stats["by_query_fetch"] += 1
        return items

    def _get_entities(
        self, qids: list[str], *, props: str, purpose: str = "entities"
    ) -> dict[str, dict[str, Any]]:
        """
        Fetch multiple entities in a single call.
        """
        ids = [q for q in qids if str(q).strip()]
        if not ids:
            return {}

        def _request():
            self.ratelimiter.wait()
            if purpose == "labels":
                self.stats["http_wbgetentities_labels"] += 1
            else:
                self.stats["http_wbgetentities"] += 1
            r = self._session.get(
                WIKIDATA_API_URL,
                params={
                    "action": "wbgetentities",
                    "ids": "|".join(ids),
                    "props": props,
                    "languages": "en",
                    "format": "json",
                },
                timeout=REQUEST.timeout_s,
                headers={"User-Agent": USER_AGENT},
            )
            r.raise_for_status()
            return r.json()

        data = with_retries(
            _request,
            retries=RETRY.retries,
            on_fail_return=None,
            context="Wikidata wbgetentities",
            retry_stats=self.stats,
        )
        if not isinstance(data, dict):
            if int(self.stats.get("network_failures", 0) or 0) > 0:
                raise RuntimeError(
                    "Wikidata request failed due to network issues (wbgetentities). "
                    "Enable internet access or rerun with a warm cache."
                )
            return {}
        entities = data.get("entities") or {}
        if not isinstance(entities, dict):
            return {}
        return {str(k): v for k, v in entities.items() if isinstance(v, dict)}

    def _collect_linked_ids(self, entity: dict[str, Any]) -> set[str]:
        claims = entity.get("claims") or {}
        if not isinstance(claims, dict):
            return set()

        out: set[str] = set()
        for prop in ("P178", "P123", "P400", "P179", "P136", "P31"):
            vals = claims.get(prop) or []
            if not isinstance(vals, list):
                continue
            for st in vals:
                m = (st or {}).get("mainsnak") or {}
                dv = m.get("datavalue") or {}
                v = dv.get("value")
                if isinstance(v, dict) and v.get("id"):
                    qid = str(v.get("id") or "").strip()
                    if qid:
                        out.add(qid)
        return out

    def _ensure_labels(self, qids: set[str]) -> None:
        if self._labels_fetch_disabled:
            raise RuntimeError(
                "Wikidata label fetching is disabled for this run due to a prior network failure. "
                "Enable internet access or rerun with a warm cache."
            )
        ids = [str(q).strip() for q in qids if str(q).strip()]
        if not ids:
            return
        missing = [q for q in ids if q not in self._labels]
        if not missing:
            self.stats["labels_hit"] += len(ids)
            return

        # Count hits for known IDs, and fetch only missing.
        self.stats["labels_hit"] += len(ids) - len(missing)
        updated = False
        for chunk in iter_chunks(missing, WIKIDATA.labels_batch_size):
            entities = self._get_entities(chunk, props="labels", purpose="labels")
            if not entities and chunk:
                # Network is likely unavailable; stop trying for the remainder of this run and fail
                # fast to avoid producing partially-resolved metadata.
                self._labels_fetch_disabled = True
                raise RuntimeError(
                    "Wikidata label lookup failed due to network issues. "
                    "Enable internet access or rerun with a warm cache."
                )
            for k, v in entities.items():
                lbl = str((v.get("labels") or {}).get("en", {}).get("value") or "").strip()
                if lbl:
                    self._labels[str(k)] = lbl
                    updated = True
        self.stats["labels_fetch"] += len(missing)
        if updated:
            self._save_cache()

    def _is_complete_entity(self, entity: dict[str, Any]) -> bool:
        """
        Determine whether a cached entity is complete enough for current extraction.

        We rely on sitelinks for Wikipedia titles and claims/labels for most fields.
        Older caches may lack sitelinks; treat them as incomplete and refetch.
        """
        if not isinstance(entity, dict):
            return False
        if "claims" not in entity or "labels" not in entity:
            return False
        # Must include sitelinks so we can derive enwiki title reliably.
        return "sitelinks" in entity

    def get_by_id(self, qid: str) -> dict[str, str] | None:
        q = str(qid or "").strip()
        if not q:
            return None
        if q in self._by_id_negative:
            self.stats["by_id_negative_hit"] += 1
            return None
        cached = self._by_id.get(q)
        if isinstance(cached, dict) and self._is_complete_entity(cached):
            self.stats["by_id_hit"] += 1
            self._ensure_labels(self._collect_linked_ids(cached))
            return self._extract_fields(cached)

        entities = self._get_entities(
            [q], props="labels|descriptions|aliases|claims|sitelinks", purpose="entities"
        )
        ent = entities.get(q)
        if not isinstance(ent, dict):
            self._by_id_negative.add(q)
            self._save_cache()
            self.stats["by_id_negative_fetch"] += 1
            return None
        self._by_id[q] = ent
        self._save_cache()
        self.stats["by_id_fetch"] += 1
        self._ensure_labels(self._collect_linked_ids(ent))
        return self._extract_fields(ent)

    def get_by_ids(self, qids: list[str]) -> dict[str, dict[str, str]]:
        """
        Fetch multiple Wikidata entities and return extracted fields for each.

        Uses cache when available and batches wbgetentities calls for missing QIDs.
        """
        ids = [str(q).strip() for q in (qids or []) if str(q).strip()]
        if not ids:
            return {}

        entities_to_extract: dict[str, dict[str, Any]] = {}
        missing: list[str] = []
        for q in ids:
            if q in self._by_id_negative:
                self.stats["by_id_negative_hit"] += 1
                continue
            cached = self._by_id.get(q)
            if isinstance(cached, dict) and self._is_complete_entity(cached):
                self.stats["by_id_hit"] += 1
                entities_to_extract[q] = cached
            else:
                missing.append(q)

        if missing:
            for chunk in iter_chunks(missing, WIKIDATA.get_by_ids_batch_size):
                entities = self._get_entities(
                    chunk, props="labels|descriptions|aliases|claims|sitelinks", purpose="entities"
                )
                for q in chunk:
                    ent = entities.get(q)
                    if not isinstance(ent, dict):
                        self._by_id_negative.add(q)
                        self.stats["by_id_negative_fetch"] += 1
                        continue
                    self._by_id[q] = ent
                    self.stats["by_id_fetch"] += 1
                    entities_to_extract[q] = ent
            self._save_cache()

        linked: set[str] = set()
        for ent in entities_to_extract.values():
            linked |= self._collect_linked_ids(ent)
        self._ensure_labels(linked)

        return {qid: self._extract_fields(ent) for qid, ent in entities_to_extract.items()}

    def get_aliases(self, qid: str, *, language: str = "en", limit: int = 20) -> list[str]:
        """
        Return cached Wikidata aliases for a QID (no network).
        """
        q = str(qid or "").strip()
        if not q:
            return []
        ent = self._by_id.get(q)
        if not isinstance(ent, dict):
            return []
        aliases = ent.get("aliases") or {}
        if not isinstance(aliases, dict):
            return []
        lang_vals = aliases.get(language) or []
        if not isinstance(lang_vals, list):
            return []
        out: list[str] = []
        for it in lang_vals:
            if not isinstance(it, dict):
                continue
            v = str(it.get("value") or "").strip()
            if v:
                out.append(v)
            if len(out) >= limit:
                break
        # Dedup preserving order
        seen: set[str] = set()
        return [a for a in out if not (a in seen or seen.add(a))]

    def search(self, game_name: str, year_hint: int | None = None) -> dict[str, str] | None:
        name = str(game_name or "").strip()
        if not name:
            return None
        items = self._search(name)
        if not items:
            return None

        def _tokens(text: object) -> set[str]:
            s = str(text or "").casefold()
            return {t for t in re.findall(r"[a-z0-9]+", s) if t}

        def _has_extra_tokens(candidate_label: object) -> bool:
            # Prefer candidates that don't add new tokens beyond the personal title. This avoids
            # pinning "edition-like" or unrelated longer titles when the base title is ambiguous.
            in_tokens = _tokens(name)
            cand_tokens = _tokens(candidate_label)
            if not in_tokens or not cand_tokens:
                return False
            return bool(cand_tokens - in_tokens)

        def _desc_is_video_game(desc: object) -> bool:
            d = str(desc or "").strip().lower()
            if not d:
                return False
            return "video game" in d or "computer game" in d

        def _desc_is_unwanted_game_kind(desc: object) -> bool:
            d = str(desc or "").strip().lower()
            if not d:
                return False
            # Avoid selecting non-game pages that can still mention "video game" in their
            # description (e.g., ESRB descriptors) or demos.
            return any(
                k in d
                for k in (
                    "demo",
                    "esrb",
                    "content descriptor",
                )
            )

        def _desc_is_non_game(desc: object) -> bool:
            d = str(desc or "").strip().lower()
            if not d:
                return False
            # Strongly non-game media types.
            return any(
                k in d
                for k in (
                    "film",
                    "movie",
                    "television",
                    "tv series",
                    "anime",
                    "manga",
                    "comic",
                    "soundtrack",
                    "album",
                    "song",
                    "novel",
                    "book",
                )
            )

        def _is_valid_game_instance(inst: str) -> bool:
            s = str(inst or "").strip().lower()
            if not s:
                return False
            # Hard rejects even if they contain the word "game".
            if any(k in s for k in ("game demo", "content descriptor", "esrb content descriptor")):
                return False
            # Allow expansions/DLC/episodes as acceptable "game" items for now; they are still
            # useful identity hubs and often the closest Wikidata entry for an edition-like title.
            if any(
                k in s
                for k in (
                    "video game",
                    "computer game",
                    "expansion pack",
                    "downloadable content",
                    "dlc",
                )
            ):
                return True
            return False

        def _year_getter(obj: dict[str, Any]) -> int | None:
            desc = str(obj.get("description") or "").strip()
            m = re.search(r"\b(19\d{2}|20\d{2})\b", desc) if desc else None
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    return None
            return None

        def _retry_disambiguation() -> dict[str, str] | None:
            # Wikidata has many short/ambiguous titles; try a conservative disambiguation
            # suffix that often surfaces the correct video game entry.
            if "video game" in name.lower():
                return None
            for alt in (f"{name} video game", f"{name} (video game)"):
                got = self.search(alt, year_hint=year_hint)
                if got:
                    return got
            return None

        def _try_instance_of_filtered_choice() -> tuple[
            dict[str, Any] | None, int, list[tuple[str, int]]
        ]:
            # If search descriptions are unhelpful (or misleading), validate a small set of top
            # candidates by checking Wikidata "instance of" (P31) from cached/batched entity
            # details. This avoids selecting films/albums/demos with the same label.
            scored_items = sorted(
                items,
                key=lambda it: fuzzy_score(name, str(it.get("label") or "")),
                reverse=True,
            )[:10]
            qids: list[str] = []
            for it in scored_items:
                q = str(it.get("id") or "").strip()
                if q:
                    qids.append(q)
            if not qids:
                return (None, -1, [])
            details_by_id = self.get_by_ids(qids)
            valid_qids = {
                qid
                for qid, det in details_by_id.items()
                if _is_valid_game_instance(str(det.get("Wikidata_InstanceOf", "")))
            }
            if not valid_qids:
                return (None, -1, [])
            valid_items = [it for it in items if str(it.get("id") or "").strip() in valid_qids]
            return pick_best_match(
                name,
                valid_items,
                name_key="label",
                year_hint=year_hint,
                year_getter=_year_getter,
            )

        # Prefer candidates whose *search result description* indicates they are a video game.
        # This avoids common traps like films/albums sharing the same title.
        preferred = [
            it
            for it in items
            if _desc_is_video_game(it.get("description"))
            and not _desc_is_unwanted_game_kind(it.get("description"))
        ]
        gameish = [it for it in items if _desc_is_video_game(it.get("description"))]
        best = None
        score = -1
        top_matches: list[tuple[str, int]] = []
        if preferred:
            best, score, top_matches = pick_best_match(
                name,
                preferred,
                name_key="label",
                year_hint=year_hint,
                year_getter=_year_getter,
            )
        if best is None or score < MATCHING.min_score:
            if gameish:
                best, score, top_matches = pick_best_match(
                    name,
                    gameish,
                    name_key="label",
                    year_hint=year_hint,
                    year_getter=_year_getter,
                )
            else:
                best, score, top_matches = pick_best_match(
                    name,
                    items,
                    name_key="label",
                    year_hint=year_hint,
                    year_getter=_year_getter,
                )

        # If the best candidate adds extra tokens (usually a subtitle/edition), try to re-rank
        # among "no-extra-token" candidates when the score is not very high.
        if best and score < MATCHING.suspicious_score and _has_extra_tokens(best.get("label")):
            safer = [it for it in items if not _has_extra_tokens(it.get("label"))]
            if safer:
                best2, score2, top2 = pick_best_match(
                    name,
                    safer,
                    name_key="label",
                    year_hint=year_hint,
                    year_getter=_year_getter,
                )
                if best2 and score2 >= MATCHING.min_score:
                    best, score, top_matches = best2, score2, top2
        if not best or score < MATCHING.min_score:
            if top_matches:
                top = ", ".join(f"'{n}' ({s}%)" for n, s in top_matches[:5])
                logging.warning(f"Not found in Wikidata: '{name}'. Closest matches: {top}")
            else:
                logging.warning(f"Not found in Wikidata: '{name}'. No matches found.")
            retry = _retry_disambiguation()
            if retry:
                return retry
            return None

        if _desc_is_non_game(best.get("description")):
            inst_best, inst_score, inst_top = _try_instance_of_filtered_choice()
            if inst_best and inst_score >= MATCHING.min_score:
                best, score, top_matches = inst_best, inst_score, inst_top
            else:
                # Try to recover by selecting another high-scoring candidate that looks like a video
                # game. This is intentionally conservative: if Wikidata doesn't clearly indicate a
                # video game, treat it as not found to avoid polluting the row with wrong metadata.
                gameish2 = [it for it in items if _desc_is_video_game(it.get("description"))]
                if gameish2:
                    alt, alt_score, alt_top = pick_best_match(
                        name,
                        gameish2,
                        name_key="label",
                        year_hint=year_hint,
                        year_getter=_year_getter,
                    )
                    if alt and alt_score >= MATCHING.min_score:
                        best = alt
                        score = alt_score
                        top_matches = alt_top
                    else:
                        logging.warning(
                            "Rejecting non-game Wikidata match for '%s' (picked '%s'); "
                            "no suitable video game candidate found.",
                            name,
                            best.get("label", ""),
                        )
                        return None
                else:
                    logging.warning(
                        "Rejecting non-game Wikidata match for '%s' (picked '%s'); "
                        "no video game candidates in search results.",
                        name,
                        best.get("label", ""),
                    )
                    return None

        if score < 100:
            msg = (
                f"Close match for '{name}': Selected '{best.get('label', '')}' (score: {score}%)"
            )
            if top_matches:
                top = ", ".join(f"'{n}' ({s}%)" for n, s in top_matches[:5])
                msg += f", alternatives: {top}"
            logging.warning(msg)

        qid = str(best.get("id") or "").strip()
        if not qid:
            return None
        details = self.get_by_id(qid)
        if not details:
            return None
        # Final guard: if "instance of" doesn't include "video game" and the description isn't
        # gameish, reject as not found.
        inst = str(details.get("Wikidata_InstanceOf", "") or "").strip().lower()
        if any(k in inst for k in ("game demo", "content descriptor")):
            logging.warning(
                "Rejecting non-game Wikidata entity for '%s' (QID %s, instance_of='%s').",
                name,
                qid,
                inst,
            )
            # Try to find another candidate that is a proper video game (not demo/descriptor).
            inst_best, inst_score, inst_top = _try_instance_of_filtered_choice()
            if inst_best and inst_score >= MATCHING.min_score:
                qid2 = str(inst_best.get("id") or "").strip()
                if qid2 and qid2 != qid:
                    details2 = self.get_by_id(qid2)
                    if details2:
                        return details2
            retry = _retry_disambiguation()
            if retry:
                return retry
            return None
        if (
            not _is_valid_game_instance(inst)
            and not _desc_is_video_game(best.get("description"))
            and not _desc_is_video_game(details.get("Wikidata_Description", ""))
        ):
            logging.warning(
                "Rejecting non-video-game Wikidata entity for '%s' (QID %s, instance_of='%s').",
                name,
                qid,
                inst,
            )
            return None
        return details

    def _extract_fields(self, entity: dict[str, Any]) -> dict[str, str]:
        qid = str(entity.get("id") or "").strip()

        def _lang_value(obj: Any, preferred: tuple[str, ...] = ("en", "en-gb", "en-ca")) -> str:
            if not isinstance(obj, dict):
                return ""
            for k in preferred:
                v = obj.get(k)
                if isinstance(v, dict):
                    s = str(v.get("value") or "").strip()
                    if s:
                        return s
            for v in obj.values():
                if isinstance(v, dict):
                    s = str(v.get("value") or "").strip()
                    if s:
                        return s
            return ""

        label = _lang_value(entity.get("labels") or {})
        desc = _lang_value(entity.get("descriptions") or {})

        # Claims helpers
        claims = entity.get("claims") or {}

        def _qids(prop: str) -> list[str]:
            out: list[str] = []
            if not isinstance(claims, dict):
                return out
            vals = claims.get(prop) or []
            if not isinstance(vals, list):
                return out
            for st in vals:
                m = (st or {}).get("mainsnak") or {}
                dv = m.get("datavalue") or {}
                v = dv.get("value")
                if isinstance(v, dict) and v.get("id"):
                    out.append(str(v.get("id")))
            return out

        def _quantity(prop: str) -> str:
            vals = claims.get(prop) or []
            if not isinstance(vals, list):
                return ""
            for st in vals:
                m = (st or {}).get("mainsnak") or {}
                dv = m.get("datavalue") or {}
                v = dv.get("value")
                if isinstance(v, dict) and "amount" in v:
                    amount = str(v.get("amount") or "").strip()
                    unit = str(v.get("unit") or "").strip()
                    if amount:
                        # Normalize "+1234" to "1234" where safe.
                        if amount.startswith("+"):
                            amount = amount[1:]
                        if unit and unit != "1":
                            return f"{amount} {unit}"
                        return amount
            return ""

        def _time_date(prop: str) -> str:
            vals = claims.get(prop) or []
            if not isinstance(vals, list):
                return ""
            for st in vals:
                m = (st or {}).get("mainsnak") or {}
                dv = m.get("datavalue") or {}
                v = dv.get("value")
                if isinstance(v, dict) and v.get("time"):
                    t = str(v.get("time") or "")
                    # "+1996-07-31T00:00:00Z"
                    if len(t) >= 11 and t[0] in {"+", "-"} and t[5] == "-" and t[8] == "-":
                        y = t[1:5]
                        m2 = t[6:8]
                        d2 = t[9:11]
                        if y.isdigit() and m2.isdigit() and d2.isdigit():
                            return f"{y}-{m2}-{d2}"
            return ""

        def _time_year(prop: str) -> str:
            vals = claims.get(prop) or []
            if not isinstance(vals, list):
                return ""
            for st in vals:
                m = (st or {}).get("mainsnak") or {}
                dv = m.get("datavalue") or {}
                v = dv.get("value")
                if isinstance(v, dict) and v.get("time"):
                    t = str(v.get("time") or "")
                    # "+1996-07-31T00:00:00Z"
                    if len(t) >= 5 and t[0] in {"+", "-"}:
                        y = t[1:5]
                        if y.isdigit():
                            return y
                    if len(t) >= 4 and t[:4].isdigit():
                        return t[:4]
            return ""

        release_year = _time_year("P577")  # publication date (year)
        release_date = _time_date("P577")  # publication date (YYYY-MM-DD when available)

        linked_ids = self._collect_linked_ids(entity)
        label_map: dict[str, str] = {
            q: str(self._labels.get(q) or "").strip() for q in linked_ids if str(q).strip()
        }

        def _labels(prop: str) -> str:
            ids = _qids(prop)
            names = [label_map.get(i, "") for i in ids]
            names = [n for n in names if n]
            return ", ".join(sorted(dict.fromkeys(names)))

        wikipedia = ""
        enwiki_title = ""
        sitelinks = entity.get("sitelinks") or {}
        if isinstance(sitelinks, dict):
            enwiki = sitelinks.get("enwiki") or {}
            if isinstance(enwiki, dict) and enwiki.get("title"):
                enwiki_title = str(enwiki.get("title"))
                wikipedia = f"https://en.wikipedia.org/wiki/{enwiki_title.replace(' ', '_')}"

        if not label and enwiki_title:
            label = enwiki_title

        return {
            "Wikidata_QID": qid,
            "Wikidata_Label": label,
            "Wikidata_Description": desc,
            "Wikidata_ReleaseYear": release_year,
            "Wikidata_ReleaseDate": release_date,
            "Wikidata_Developers": _labels("P178"),
            "Wikidata_Publishers": _labels("P123"),
            "Wikidata_Platforms": _labels("P400"),
            "Wikidata_Series": _labels("P179"),
            "Wikidata_Genres": _labels("P136"),
            "Wikidata_InstanceOf": _labels("P31"),
            "Wikidata_EnwikiTitle": enwiki_title,
            "Wikidata_Wikipedia": wikipedia,
        }

    def format_cache_stats(self) -> str:
        s = self.stats
        base = (
            f"by_query hit={s['by_query_hit']} fetch={s['by_query_fetch']} "
            f"(neg hit={s['by_query_negative_hit']} fetch={s['by_query_negative_fetch']}), "
            f"by_hint hit={s['by_hint_hit']} fetch={s['by_hint_fetch']} "
            f"(neg hit={s['by_hint_negative_hit']} fetch={s['by_hint_negative_fetch']}), "
            f"by_id hit={s['by_id_hit']} fetch={s['by_id_fetch']} "
            f"(neg hit={s['by_id_negative_hit']} fetch={s['by_id_negative_fetch']}), "
            f"labels hit={s['labels_hit']} fetch={s['labels_fetch']}, "
            f"http search={s['http_wbsearchentities']} getentities={s['http_wbgetentities']} "
            f"labels={s['http_wbgetentities_labels']} sparql={s['http_sparql']}"
        )
        base += (
            f", cache load_ms={int(s.get('cache_load_ms', 0) or 0)}"
            f" save_ms={int(s.get('cache_save_ms', 0) or 0)}"
            f" saves={int(s.get('cache_save_count', 0) or 0)}"
        )
        http_429 = int(s.get("http_429", 0) or 0)
        if http_429:
            return (
                base
                + f", 429={http_429} retries={int(s.get('http_429_retries', 0) or 0)}"
                + f" backoff_ms={int(s.get('http_429_backoff_ms', 0) or 0)}"
            )
        return base
