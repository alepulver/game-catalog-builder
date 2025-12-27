from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from ..utils.company import company_key, normalize_company_name


@dataclass(frozen=True)
class NormalizeProductionTiersResult:
    publishers_in: int
    developers_in: int
    publishers_out: int
    developers_out: int
    publishers_merged: int
    developers_merged: int
    publishers_conflicts: int
    developers_conflicts: int


_TIER_RANK = {"Indie": 1, "AA": 2, "AAA": 3}


def _tier_rank(tier: str) -> int:
    return _TIER_RANK.get(tier, 0)


def _pick_canonical_label(labels: list[str]) -> str:
    """
    Pick a readable canonical label among variants that map to the same `company_key`.
    This is intentionally heuristic; it only affects the YAML key string, not matching.
    """

    def score(label: str) -> tuple[int, int, int, int, str]:
        s = normalize_company_name(label)
        # Lower score is better.
        penalty = 0
        if not s:
            penalty += 100
        # Prefer not ALLCAPS / not alllower.
        if s.isupper():
            penalty += 20
        if s.islower():
            penalty += 5
        # Prefer spaces over hyphenated variants (e.g. "Eidos Montréal" over "Eidos-Montréal").
        penalty += 2 if "-" in s else 0
        # Penalize obvious emoji / symbols (keep readable ASCII-ish labels).
        penalty += 15 if any(ord(ch) > 0xFFFF for ch in s) else 0
        penalty += 10 if "🚀" in s else 0
        penalty += 5 if "™" in s else 0
        penalty += 5 if "®" in s else 0
        # Prefer shorter punctuation.
        punct = sum(1 for ch in s if ch in '!?;:"')
        penalty += punct * 2
        # Prefer longer (avoid overly short/abbreviated) when tied.
        return (penalty, -len(s), s.count(" "), s.count("&"), s.casefold())

    return sorted(labels, key=score)[0]


def _load_yaml_mapping(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {"publishers": {}, "developers": {}}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        return {"publishers": {}, "developers": {}}
    pubs = data.get("publishers")
    devs = data.get("developers")
    pubs = pubs if isinstance(pubs, dict) else {}
    devs = devs if isinstance(devs, dict) else {}

    def _as_tier_map(m: dict[str, Any]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in m.items():
            tier = ""
            if isinstance(v, dict):
                tier = str(v.get("tier") or "").strip()
            elif isinstance(v, str):
                tier = v.strip()
            if tier in _TIER_RANK:
                out[str(k)] = tier
        return out

    return {"publishers": _as_tier_map(pubs), "developers": _as_tier_map(devs)}


def normalize_production_tiers_yaml(
    *, in_yaml: Path, out_yaml: Path | None = None
) -> NormalizeProductionTiersResult:
    """
    Deduplicate a production tiers YAML by `company_key` and rewrite it in a canonical form.

    This only rewrites the mapping file (YAML keys). Enrich matching uses `company_key`, so this is
    purely for cleanliness and to avoid repeated variants in the local mapping.
    """
    out = out_yaml or in_yaml
    mapping = _load_yaml_mapping(in_yaml)

    def normalize_section(section: dict[str, str]) -> tuple[dict[str, str], int, int]:
        by_key: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for label, tier in section.items():
            key = company_key(label)
            if not key:
                continue
            by_key[key].append((label, tier))

        merged = 0
        conflicts = 0
        out_map: dict[str, str] = {}

        for _key, items in by_key.items():
            labels = [label for label, _ in items]
            tiers = [tier for _, tier in items]
            canonical = _pick_canonical_label(labels)

            tier_best = max(tiers, key=_tier_rank)
            if len(set(labels)) > 1:
                merged += 1
            if len({t for t in tiers if t in _TIER_RANK}) > 1:
                conflicts += 1
            out_map[canonical] = tier_best

        # Deterministic order for diffs/readability.
        out_map = dict(sorted(out_map.items(), key=lambda kv: kv[0].casefold()))
        return (out_map, merged, conflicts)

    pubs_out, pubs_merged, pubs_conflicts = normalize_section(mapping["publishers"])
    devs_out, devs_merged, devs_conflicts = normalize_section(mapping["developers"])

    out.write_text(
        yaml.safe_dump(
            {"publishers": pubs_out, "developers": devs_out},
            sort_keys=False,
            allow_unicode=True,
            width=100,
        ),
        encoding="utf-8",
    )

    return NormalizeProductionTiersResult(
        publishers_in=len(mapping["publishers"]),
        developers_in=len(mapping["developers"]),
        publishers_out=len(pubs_out),
        developers_out=len(devs_out),
        publishers_merged=pubs_merged,
        developers_merged=devs_merged,
        publishers_conflicts=pubs_conflicts,
        developers_conflicts=devs_conflicts,
    )
