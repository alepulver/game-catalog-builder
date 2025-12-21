from __future__ import annotations

import json
import logging
import re
import time
from collections import Counter
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
import yaml


def _split_csv_list(value: Any) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    if raw.casefold() in {"nan", "none", "null"}:
        return []
    if not raw.startswith("["):
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(x).strip() for x in parsed if str(x).strip()]


def _yaml_quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


# Small built-in seed mapping to ensure well-known entities are tiered even if the YAML is empty.
# This is intentionally short and should evolve via the YAML file over time.
KNOWN_PUBLISHER_TIERS: dict[str, str] = {
    "Activision": "AAA",
    "Bethesda Softworks": "AAA",
    "Capcom": "AAA",
    "CD PROJEKT RED": "AAA",
    "Disney": "AAA",
    "Electronic Arts": "AAA",
    "Epic Games": "AAA",
    "KONAMI": "AAA",
    "LucasArts": "AAA",
    "PlayStation Publishing LLC": "AAA",
    "Rockstar Games": "AAA",
    "SEGA": "AAA",
    "Square Enix": "AAA",
    "Ubisoft": "AAA",
    "Valve": "AAA",
    "Warner Bros. Games": "AAA",
    "WB Games": "AAA",
    "Xbox Game Studios": "AAA",
    "2K": "AAA",
    "BANDAI NAMCO Entertainment": "AAA",
    "Bandai Namco Entertainment": "AAA",
    "BANDAI NAMCO Entertainment Europe": "AAA",
    "SNK CORPORATION": "AA",
    "Bohemia Interactive": "AA",
    "Codemasters": "AA",
    "Deep Silver": "AA",
    "Double Fine Productions": "AA",
    "Egosoft": "AA",
    "Focus Entertainment": "AA",
    "Funcom": "AA",
    "Io-Interactive A/S": "AA",
    "Marvelous USA": "AA",
    "Microids": "AA",
    "Nacon": "AA",
    "NIS America": "AA",
    "Private Division": "AA",
    "THQ Nordic": "AA",
    "505 Games": "AA",
    "Devolver Digital": "Indie",
    "Humble Games": "Indie",
    "New Blood Interactive": "Indie",
    "PLAYISM": "Indie",
    "Playdead": "Indie",
    "Supergiant Games": "Indie",
    "Team17": "Indie",
    "tinyBuild": "Indie",
}

KNOWN_DEVELOPER_TIERS: dict[str, str] = {
    "Arkane Studios": "AAA",
    "BioWare": "AAA",
    "Bungie": "AAA",
    "CAPCOM Co.": "AAA",
    "Capcom": "AAA",
    "CD PROJEKT RED": "AAA",
    "DICE": "AAA",
    "Epic Games": "AAA",
    "id Software": "AAA",
    "Infinity Ward": "AAA",
    "MachineGames": "AAA",
    "NetherRealm Studios": "AAA",
    "Obsidian Entertainment": "AAA",
    "Pandemic Studios": "AAA",
    "PlatinumGames": "AAA",
    "Respawn Entertainment": "AAA",
    "Rockstar North": "AAA",
    "Tango Gameworks": "AAA",
    "The Coalition": "AAA",
    "Toys for Bob": "AAA",
    "Treyarch": "AAA",
    "Ubisoft": "AAA",
    "Ubisoft Montreal": "AAA",
    "Ubisoft Paris": "AAA",
    "Valve": "AAA",
    "Visceral Games": "AAA",
    "4A Games": "AA",
    "Bohemia Interactive": "AA",
    "Flying Wild Hog": "AA",
    "GSC Game World": "AA",
    "Io-Interactive A/S": "AA",
    "Kunos Simulazioni": "AA",
    "Nixxes": "AA",
    "Piranha Bytes": "AA",
    "Quantic Dream": "AA",
    "Relic Entertainment": "AA",
    "Saber Interactive": "AA",
    "Slightly Mad Studios": "AA",
    "Subset Games": "Indie",
    "Deconstructeam": "Indie",
    "Frictional Games": "Indie",
    "Klei Entertainment": "Indie",
    "No Code": "Indie",
    "Oddworld Inhabitants": "Indie",
    "Playdead": "Indie",
    "Supergiant Games": "Indie",
    "Terry Cavanagh": "Indie",
    "Wales Interactive": "Indie",
}


def load_production_tiers(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {"publishers": {}, "developers": {}}
    text = path.read_text(encoding="utf-8")

    # Repair mode: if the file ever ends up with duplicate top-level keys (e.g., two `publishers:`),
    # PyYAML will keep only the last one. We merge all sections by parsing the YAML-ish blocks.
    if len(re.findall(r"(?m)^publishers:\\s*$", text)) > 1 or len(
        re.findall(r"(?m)^developers:\\s*$", text)
    ) > 1:
        pubs: dict[str, str] = {}
        devs: dict[str, str] = {}
        section: str | None = None
        for line in text.splitlines():
            if re.match(r"^publishers:\\s*$", line):
                section = "publishers"
                continue
            if re.match(r"^developers:\\s*$", line):
                section = "developers"
                continue
            # stop at other top-level keys
            if re.match(r"^[A-Za-z_][\\w-]*:\\s*$", line) and not re.match(
                r"^(publishers|developers):\\s*$", line
            ):
                section = None
                continue
            if section not in {"publishers", "developers"}:
                continue
            m = re.match(r'^\\s{2}(?P<k>\".*?\"|[^:]+):\\s*(?P<v>\".*?\"|\\w+)\\s*$', line)
            if not m:
                continue
            k_raw = m.group("k").strip()
            v_raw = m.group("v").strip()
            try:
                k = yaml.safe_load(k_raw)
            except Exception:
                k = k_raw.strip('"')
            try:
                v = yaml.safe_load(v_raw)
            except Exception:
                v = v_raw.strip('"')
            if not isinstance(k, str) or not isinstance(v, str):
                continue
            if section == "publishers":
                pubs[k] = v
            else:
                devs[k] = v
        return {"publishers": pubs, "developers": devs}

    data = yaml.safe_load(text) or {}
    pubs = data.get("publishers") if isinstance(data, dict) else {}
    devs = data.get("developers") if isinstance(data, dict) else {}
    return {
        "publishers": pubs if isinstance(pubs, dict) else {},
        "developers": devs if isinstance(devs, dict) else {},
    }


def entities_from_enriched_csv(
    enriched_csv: Path,
    *,
    min_count: int = 1,
) -> tuple[Counter[str], Counter[str]]:
    """
    Read `Steam_Publishers` and `Steam_Developers` from an enriched CSV and return:
      (publishers_counter, developers_counter)
    """
    import csv

    pubs: Counter[str] = Counter()
    devs: Counter[str] = Counter()

    with enriched_csv.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            for p in _split_csv_list(row.get("Steam_Publishers", "")):
                pubs[p] += 1
            for d in _split_csv_list(row.get("Steam_Developers", "")):
                devs[d] += 1

    if min_count > 1:
        pubs = Counter({k: v for k, v in pubs.items() if v >= min_count})
        devs = Counter({k: v for k, v in devs.items() if v >= min_count})
    return (pubs, devs)


@dataclass(frozen=True)
class WikipediaPick:
    title: str
    url: str
    extract: str
    reason: str


class WikipediaClient:
    def __init__(
        self,
        *,
        wiki_cache_path: Path | None = None,
        min_interval_s: float = 0.15,
        timeout_s: float = 20.0,
        user_agent: str = "game-catalog-builder/1.0 (contact: alepulver@protonmail.com)",
    ) -> None:
        self._s = requests.Session()
        self._s.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/json",
            }
        )
        self._min_interval_s = max(0.0, float(min_interval_s))
        self._timeout_s = float(timeout_s)
        self._last_request_at = 0.0
        self._cache_path = wiki_cache_path
        self._cache: dict[str, dict[str, Any]] = {"opensearch": {}, "summary": {}}
        if self._cache_path and self._cache_path.exists():
            try:
                self._cache = json.loads(self._cache_path.read_text(encoding="utf-8"))
            except Exception:
                self._cache = {"opensearch": {}, "summary": {}}

    def _sleep_if_needed(self) -> None:
        if self._min_interval_s <= 0:
            return
        now = time.monotonic()
        remaining = self._min_interval_s - (now - self._last_request_at)
        if remaining > 0:
            time.sleep(remaining)

    def _mark_request(self) -> None:
        self._last_request_at = time.monotonic()

    def _save_cache(self) -> None:
        if not self._cache_path:
            return
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._cache_path.write_text(
            json.dumps(self._cache, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def opensearch(self, query: str, *, limit: int = 10) -> list[tuple[str, str]]:
        query = str(query or "").strip()
        if not query:
            return []
        cached = (self._cache.get("opensearch") or {}).get(query)
        if isinstance(cached, dict) and isinstance(cached.get("results"), list):
            results = cached["results"]
            return [(str(t), str(u)) for t, u in results]

        self._sleep_if_needed()
        url = "https://en.wikipedia.org/w/api.php"
        params = {
            "action": "opensearch",
            "search": query,
            "limit": int(limit),
            "namespace": 0,
            "format": "json",
        }
        r = self._s.get(url, params=params, timeout=self._timeout_s)
        self._mark_request()
        r.raise_for_status()
        data = r.json()
        titles = data[1] if len(data) > 1 else []
        urls = data[3] if len(data) > 3 else []
        results = list(zip(titles, urls))
        self._cache.setdefault("opensearch", {})[query] = {"results": results}
        self._save_cache()
        return [(str(t), str(u)) for t, u in results]

    def search(self, query: str, *, limit: int = 10) -> list[str]:
        """
        MediaWiki search API. Usually more reliable than opensearch for company names.
        Returns a list of page titles (best first).
        """
        query = str(query or "").strip()
        if not query:
            return []
        cached = (self._cache.get("search") or {}).get(query)
        if isinstance(cached, dict) and isinstance(cached.get("titles"), list):
            return [str(t) for t in cached["titles"]]

        self._sleep_if_needed()
        url = "https://en.wikipedia.org/w/api.php"
        params = {
            "action": "query",
            "list": "search",
            "srsearch": query,
            "srlimit": int(limit),
            "srnamespace": 0,
            "format": "json",
        }
        r = self._s.get(url, params=params, timeout=self._timeout_s)
        self._mark_request()
        r.raise_for_status()
        j = r.json()
        titles = [it.get("title") for it in (j.get("query", {}).get("search") or [])]
        titles = [t for t in titles if isinstance(t, str) and t.strip()]
        self._cache.setdefault("search", {})[query] = {"titles": titles}
        self._save_cache()
        return [str(t) for t in titles]

    def summary(self, title: str) -> tuple[str, str]:
        title = str(title or "").strip()
        if not title:
            return ("", "")
        cached = (self._cache.get("summary") or {}).get(title)
        if isinstance(cached, dict):
            return (str(cached.get("extract") or ""), str(cached.get("url") or ""))

        self._sleep_if_needed()
        url = "https://en.wikipedia.org/api/rest_v1/page/summary/" + quote(title)
        r = self._s.get(url, timeout=self._timeout_s)
        self._mark_request()
        r.raise_for_status()
        j = r.json()
        extract = str(j.get("extract") or "")
        page_url = str(j.get("content_urls", {}).get("desktop", {}).get("page") or "")
        self._cache.setdefault("summary", {})[title] = {"extract": extract, "url": page_url}
        self._save_cache()
        return (extract, page_url)


_BAD_TITLE_HINTS = (
    "(film)",
    "(song)",
    "(novel)",
    "(album)",
    "(band)",
    "(politician)",
)

_ORG_EXTRACT_HINTS = (
    "video game developer",
    "video game publisher",
    "video game company",
    "game developer",
    "game publisher",
    "developer and publisher",
    "independent video game developer",
)

_DISAMBIGUATION_HINTS = (
    "may refer to",
    "can refer to",
    "may also refer to",
)

_MAJOR_PARENTS = (
    "microsoft",
    "xbox game studios",
    "sony",
    "playstation",
    "nintendo",
    "tencent",
    "sega",
    "take-two",
    "2k",
    "electronic arts",
    "ea",
    "ubisoft",
    "embracer",
    "square enix",
    "bandai namco",
    "warner bros",
    "capcom",
    "konami",
    "bethesda",
)


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").casefold()).strip()


def _title_score(name: str, title: str) -> float:
    n = _norm(name)
    t = _norm(title)
    if not n or not t:
        return 0.0
    score = 0.0
    if n == t:
        score += 2.0
    if n in t or t in n:
        score += 0.6
    score += SequenceMatcher(None, n, t).ratio()
    # similarity proxy: shared tokens
    n_tokens = set(n.split())
    t_tokens = set(t.split())
    if n_tokens and t_tokens:
        score += len(n_tokens & t_tokens) / max(len(n_tokens), len(t_tokens))
    if any(h in title for h in _BAD_TITLE_HINTS):
        score -= 0.6
    return score


def pick_wikipedia_page(
    *,
    client: WikipediaClient,
    entity_name: str,
    entity_type: str,
) -> WikipediaPick | None:
    """
    Find a best-effort Wikipedia page for a publisher/developer name.
    """
    # Try a small set of query variants (learned from earlier batch):
    # - generic names need a strong "video game" hint
    # - acronyms/tiny tokens need extra context
    variants: list[str] = []
    base = entity_name.strip()
    if not base:
        return None
    variants.append(base)
    variants.append(f"{base} video game")
    if entity_type == "publisher":
        variants.append(f"{base} video game publisher")
        variants.append(f"{base} publisher")
    else:
        variants.append(f"{base} video game developer")
        variants.append(f"{base} game developer")
        variants.append(f"{base} developer")

    # Special hard cases from our earlier runs.
    if entity_name in {"a.s.", "sen"}:
        variants = [f"{base} video game developer"] + variants
    if entity_name in {"Monolith", "Ion Storm"}:
        variants = [f"{base} video game company"] + variants

    # Collect candidates from MediaWiki search (titles only), then opensearch as fallback.
    title_candidates: list[str] = []
    for q in variants:
        title_candidates.extend(client.search(q, limit=10))
        if len(title_candidates) >= 15:
            break
    if not title_candidates:
        # opensearch fallback
        for q in variants:
            title_candidates.extend([t for t, _ in client.opensearch(q, limit=10)])
            if len(title_candidates) >= 15:
                break
    if not title_candidates:
        return None

    # Deduplicate while keeping order.
    seen: set[str] = set()
    unique_titles: list[str] = []
    for t in title_candidates:
        if t in seen:
            continue
        seen.add(t)
        unique_titles.append(t)

    scored = sorted(((_title_score(entity_name, t), t) for t in unique_titles), reverse=True)

    # Try top-N candidates and pick first whose extract looks like a game org.
    for score, title in scored[:8]:
        try:
            extract, page_url = client.summary(title)
        except Exception:
            continue
        e = _norm(extract)
        if any(_norm(h) in e for h in _ORG_EXTRACT_HINTS) and score >= 1.2:
            return WikipediaPick(
                title=title,
                url=page_url,
                extract=extract,
                reason=f"extract_match(score={score:.3f})",
            )

    # Fall back to best title similarity, but only if it isn't obviously unrelated.
    best_score, best_title = scored[0]
    try:
        extract, page_url = client.summary(best_title)
    except Exception:
        return None
    extract_cf = (extract or "").casefold()
    if any(h in extract_cf for h in _DISAMBIGUATION_HINTS):
        return None
    if extract and (
        "video game" in extract_cf or "developer" in extract_cf or "publisher" in extract_cf
    ):
        return WikipediaPick(
            title=best_title,
            url=page_url,
            extract=extract,
            reason=f"fallback_similarity(score={best_score:.3f})",
        )
    return None


def suggest_tier_from_wikipedia_extract(extract: str, *, entity_type: str) -> tuple[str, str]:
    t = (extract or "").casefold()
    if not t:
        return ("", "no_extract")

    if "independent" in t or "indie" in t:
        return ("Indie", "extract:independent")

    if "first-party" in t:
        return ("AAA", "extract:first_party")

    if "subsidiary" in t or "owned by" in t:
        if any(p in t for p in _MAJOR_PARENTS):
            return ("AAA", "extract:owned_by_major")

    # If it's clearly a game company but not clearly indie/major, default to AA.
    if any(h in t for h in _ORG_EXTRACT_HINTS):
        return ("AA", "extract:game_company_default_aa")

    return ("", "extract:unknown")


def update_production_tiers_yaml_in_place(
    yaml_path: Path,
    *,
    add_publishers: dict[str, str],
    add_developers: dict[str, str],
) -> None:
    """
    Update `data/production_tiers.yaml`.

    This intentionally rewrites the YAML (deterministic, single `publishers` + `developers`).
    Preserving comments is not worth the risk of producing duplicate top-level keys, which breaks
    parsing (PyYAML keeps the last duplicate key, dropping earlier data).
    """
    existing = load_production_tiers(yaml_path)
    pubs_in = dict(existing.get("publishers", {}))
    devs_in = dict(existing.get("developers", {}))
    pubs_in.update(add_publishers or {})
    devs_in.update(add_developers or {})
    from game_catalog_builder.utils.signals import normalize_company_name

    def _tier_rank(t: str) -> int:
        tt = str(t or "").strip()
        if tt == "AAA":
            return 3
        if tt == "AA":
            return 2
        if tt == "Indie":
            return 1
        if tt == "Unknown":
            return 0
        return -1

    def _pick_key(current_key: str, new_key: str) -> str:
        # Prefer canonical-ish names (from known seed mappings), then shorter, then stable order.
        if current_key in KNOWN_PUBLISHER_TIERS or current_key in KNOWN_DEVELOPER_TIERS:
            return current_key
        if new_key in KNOWN_PUBLISHER_TIERS or new_key in KNOWN_DEVELOPER_TIERS:
            return new_key
        if len(new_key) < len(current_key):
            return new_key
        if len(new_key) > len(current_key):
            return current_key
        return min(current_key, new_key)

    def _collapse(items: dict[str, str]) -> dict[str, str]:
        # Collapse keys that normalize to the same company name.
        collapsed: dict[str, tuple[str, str]] = {}  # norm -> (tier, key)
        for k, v in items.items():
            nk = normalize_company_name(k)
            tier = str(v or "").strip()
            if not nk or not tier:
                continue
            norm_key = nk.casefold()
            prev = collapsed.get(norm_key)
            if prev is None:
                collapsed[norm_key] = (tier, str(k))
                continue
            prev_tier, prev_key = prev
            if _tier_rank(tier) > _tier_rank(prev_tier):
                collapsed[norm_key] = (tier, str(k))
            elif _tier_rank(tier) == _tier_rank(prev_tier):
                collapsed[norm_key] = (prev_tier, _pick_key(prev_key, str(k)))
        out: dict[str, str] = {}
        for _norm, (tier, key) in collapsed.items():
            out[key] = tier
        return dict(sorted(out.items()))

    pubs = _collapse(pubs_in)
    devs = _collapse(devs_in)

    data = {"publishers": pubs, "developers": devs}

    header = (
        "# Production tier mapping (project-specific).\n"
        "#\n"
        "# Used to populate:\n"
        "#   - `Production_Tier`\n"
        "#   - `Production_TierReason`\n"
        "#\n"
        "# Keys match exact Steam metadata strings in `Steam_Publishers` / `Steam_Developers`.\n"
        "#\n"
        "# Tiers are intentionally coarse:\n"
        "#   - AAA: major publishers / large studios\n"
        "#   - AA: mid-size publishers/studios\n"
        "#   - Indie: indie publishers / small-to-mid studios\n"
        "#\n"
        "# Generated/updated by `python run.py production-tiers ...`.\n"
    )

    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    yaml_path.write_text(header + yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


@dataclass(frozen=True)
class UpdateResult:
    added_publishers: int
    added_developers: int
    defaulted_publishers: int
    defaulted_developers: int
    unknown_publishers: int
    unknown_developers: int
    unresolved: int
    conflicts: int


def suggest_and_update_production_tiers(
    *,
    enriched_csv: Path,
    yaml_path: Path,
    wiki_cache_path: Path,
    apply: bool,
    max_items: int = 50,
    min_count: int = 2,
    update_existing: bool = False,
    min_interval_s: float = 0.15,
    ensure_complete: bool = True,
    wiki_client: WikipediaClient | None = None,
    include_known_seeds: bool = True,
    include_porting_labels: bool = True,
    unknown_tier: str = "Unknown",
) -> UpdateResult:
    from game_catalog_builder.utils.signals import normalize_company_name

    mapping = load_production_tiers(yaml_path)
    if include_known_seeds:
        existing_pubs = {**KNOWN_PUBLISHER_TIERS, **mapping["publishers"]}
        existing_devs = {**KNOWN_DEVELOPER_TIERS, **mapping["developers"]}
        seed_add_pubs = {
            k: v for k, v in KNOWN_PUBLISHER_TIERS.items() if k not in mapping["publishers"]
        }
        seed_add_devs = {
            k: v for k, v in KNOWN_DEVELOPER_TIERS.items() if k not in mapping["developers"]
        }
    else:
        existing_pubs = dict(mapping["publishers"])
        existing_devs = dict(mapping["developers"])
        seed_add_pubs = {}
        seed_add_devs = {}

    pubs_counter, devs_counter = entities_from_enriched_csv(enriched_csv, min_count=min_count)

    porting_prefixes = ("Feral Interactive", "Aspyr")
    missing_pubs: dict[str, int] = {}
    missing_devs: dict[str, int] = {}
    for name, count in pubs_counter.items():
        norm = normalize_company_name(name) or str(name)
        if not include_porting_labels and norm.startswith(porting_prefixes):
            continue
        current = str(mapping["publishers"].get(norm, "") or "").strip()
        current_is_unknown = bool(current) and current.casefold() == str(unknown_tier).casefold()
        if update_existing or norm not in existing_pubs or current_is_unknown:
            missing_pubs[norm] = missing_pubs.get(norm, 0) + int(count)
    for name, count in devs_counter.items():
        norm = normalize_company_name(name) or str(name)
        if not include_porting_labels and norm.startswith(porting_prefixes):
            continue
        current = str(mapping["developers"].get(norm, "") or "").strip()
        current_is_unknown = bool(current) and current.casefold() == str(unknown_tier).casefold()
        if update_existing or norm not in existing_devs or current_is_unknown:
            missing_devs[norm] = missing_devs.get(norm, 0) + int(count)

    wiki_candidates: list[tuple[int, str, str]] = []
    for name, count in missing_pubs.items():
        wiki_candidates.append((count, "publisher", name))
    for name, count in missing_devs.items():
        wiki_candidates.append((count, "developer", name))
    wiki_candidates.sort(key=lambda x: (x[0], x[2]), reverse=True)
    wiki_candidates = wiki_candidates[: max(0, int(max_items))]

    client = wiki_client or WikipediaClient(
        wiki_cache_path=wiki_cache_path, min_interval_s=min_interval_s
    )

    add_pubs: dict[str, str] = {}
    add_devs: dict[str, str] = {}
    defaulted_pubs = 0
    defaulted_devs = 0
    unknown_pubs = 0
    unknown_devs = 0
    unresolved = 0
    conflicts = 0
    network_errors = 0

    # Seed additions first (no network).
    add_pubs.update(seed_add_pubs)
    add_devs.update(seed_add_devs)

    processed: set[tuple[str, str]] = set()
    for count, entity_type, name in wiki_candidates:
        processed.add((entity_type, name))
        try:
            pick = pick_wikipedia_page(client=client, entity_name=name, entity_type=entity_type)
        except Exception as e:
            if isinstance(e, requests.exceptions.RequestException):
                network_errors += 1
            logging.warning(f"[tiers] wikipedia_error for {entity_type} {name!r}: {e!r}")
            pick = None
        if not pick:
            unresolved += 1
            if ensure_complete:
                tier = str(unknown_tier or "").strip()
                if entity_type == "publisher":
                    add_pubs[name] = tier
                    unknown_pubs += 1
                else:
                    add_devs[name] = tier
                    unknown_devs += 1
                logging.warning(f"[tiers] {entity_type} {name!r} -> {tier} (count={count})")
            else:
                logging.info(f"[tiers] unresolved {entity_type} {name!r} (count={count})")
            continue
        tier, reason = suggest_tier_from_wikipedia_extract(pick.extract, entity_type=entity_type)
        if not tier:
            unresolved += 1
            if ensure_complete:
                tier = str(unknown_tier or "").strip()
                if entity_type == "publisher":
                    add_pubs[name] = tier
                    unknown_pubs += 1
                else:
                    add_devs[name] = tier
                    unknown_devs += 1
                logging.warning(
                    f"[tiers] {entity_type} {name!r} -> {tier} (count={count}) wiki={pick.url}"
                )
            else:
                logging.info(
                    f"[tiers] unresolved {entity_type} {name!r} (count={count}) wiki={pick.url}"
                )
            continue

        if entity_type == "publisher":
            prev = existing_pubs.get(name, "")
            if prev == unknown_tier:
                prev = ""
            if prev and prev != tier and not update_existing:
                conflicts += 1
                logging.warning(
                    f"[tiers] conflict publisher {name!r}: yaml={prev!r} wiki={tier!r} ({reason})"
                )
                continue
            add_pubs[name] = tier
        else:
            prev = existing_devs.get(name, "")
            if prev == unknown_tier:
                prev = ""
            if prev and prev != tier and not update_existing:
                conflicts += 1
                logging.warning(
                    f"[tiers] conflict developer {name!r}: yaml={prev!r} wiki={tier!r} ({reason})"
                )
                continue
            add_devs[name] = tier

        logging.info(f"[tiers] {entity_type} {name!r} -> {tier} ({reason}) wiki={pick.url}")

    # Ensure completeness without requiring huge network runs: after we've attempted Wikipedia for
    # the most frequent entities, fill the rest with defaults (optional).
    if ensure_complete and network_errors == 0:
        for name in sorted(missing_pubs.keys()):
            if ("publisher", name) in processed:
                continue
            if not update_existing and name in existing_pubs:
                continue
            if name in add_pubs:
                continue
            add_pubs[name] = str(unknown_tier or "").strip()
            unknown_pubs += 1
        for name in sorted(missing_devs.keys()):
            if ("developer", name) in processed:
                continue
            if not update_existing and name in existing_devs:
                continue
            if name in add_devs:
                continue
            add_devs[name] = str(unknown_tier or "").strip()
            unknown_devs += 1
    elif ensure_complete and network_errors > 0:
        logging.warning(
            "[tiers] Network errors detected; skipping Unknown fill to avoid poisoning the mapping."
        )

    # Only add new entries if we aren't updating existing.
    if not update_existing:
        add_pubs = {
            k: v
            for k, v in add_pubs.items()
            if k not in mapping["publishers"]
            or str(mapping["publishers"].get(k, "") or "").strip().casefold()
            == str(unknown_tier).strip().casefold()
        }
        add_devs = {
            k: v
            for k, v in add_devs.items()
            if k not in mapping["developers"]
            or str(mapping["developers"].get(k, "") or "").strip().casefold()
            == str(unknown_tier).strip().casefold()
        }

    if apply:
        update_production_tiers_yaml_in_place(
            yaml_path,
            add_publishers=add_pubs,
            add_developers=add_devs,
        )

    return UpdateResult(
        added_publishers=len(add_pubs),
        added_developers=len(add_devs),
        defaulted_publishers=defaulted_pubs,
        defaulted_developers=defaulted_devs,
        unknown_publishers=unknown_pubs,
        unknown_developers=unknown_devs,
        unresolved=unresolved,
        conflicts=conflicts,
    )
