from __future__ import annotations

import csv
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from ..utils.company import (
    LOW_SIGNAL_COMPANY_KEYS,
    company_key,
    normalize_company_name,
    parse_json_array_cell,
)


@dataclass(frozen=True)
class CollectProductionTiersResult:
    publishers_total: int
    developers_total: int


class _FlowListDumper(yaml.SafeDumper):
    pass


def _represent_flow_list(dumper: yaml.SafeDumper, data: list[Any]) -> yaml.nodes.SequenceNode:
    return dumper.represent_sequence("tag:yaml.org,2002:seq", data, flow_style=True)


_FlowListDumper.add_representer(list, _represent_flow_list)


def _tier_str(v: Any) -> str:
    if isinstance(v, dict):
        return str(v.get("tier") or "").strip()
    if isinstance(v, str):
        return v.strip()
    return ""


def _load_existing_yaml(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {"publishers": {}, "developers": {}}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {"publishers": {}, "developers": {}}
    pubs = data.get("publishers") if isinstance(data, dict) else {}
    devs = data.get("developers") if isinstance(data, dict) else {}
    return {
        "publishers": pubs if isinstance(pubs, dict) else {},
        "developers": devs if isinstance(devs, dict) else {},
    }


def _wants_company(label: str, *, include_porting_labels: bool) -> bool:
    # Wikidata sometimes yields wiki-markup-ish platform labels like "'''Dreamcast'''"; ignore.
    if "'''" in label:
        return False
    # Ignore some common non-company labels that show up in Wikidata publisher/developer fields.
    # This is intentionally conservative; the goal is to avoid polluting the tiers list.
    ignore_keys = {
        "amiga",
        "north america",
        "playstation mobile",
        "playstation pc",
        "small",
        "lda",
        "eq",
        "shanghai",
        "toronto",
        "kiev",
        "windows",
        "mac",
        "linux",
        "dreamcast",
        "gizmondo",
        "n gage",
        "game boy advance",
        "__not_found__",
        "not found",
    }
    if company_key(label) in ignore_keys:
        return False
    n = normalize_company_name(label)
    if not n:
        return False
    if n.casefold().startswith("and "):
        return False
    if not include_porting_labels and n.casefold() in LOW_SIGNAL_COMPANY_KEYS:
        return False
    return True


def collect_production_tiers_yaml(
    *,
    enriched_csv: Path,
    out_yaml: Path,
    base_yaml: Path | None = None,
    min_count: int = 1,
    max_examples: int = 6,
    include_porting_labels: bool = True,
    keep_existing: bool = True,
    only_missing: bool = False,
) -> CollectProductionTiersResult:
    """
    Scan provider publisher/developer columns from an enriched CSV and write a YAML file to
    manually curate coarse production tiers (AAA/AA/Indie).

    The YAML format is intentionally human/AI-editable and may include extra fields
    (count/examples); enrich only needs the `tier` field.
    """
    publisher_cols = (
        "Steam_Publishers",
        "IGDB_Publishers",
        "RAWG_Publishers",
        "Wikidata_Publishers",
    )
    developer_cols = (
        "Steam_Developers",
        "IGDB_Developers",
        "RAWG_Developers",
        "Wikidata_Developers",
    )

    pub_counts: Counter[str] = Counter()
    dev_counts: Counter[str] = Counter()
    pub_examples: dict[str, list[str]] = defaultdict(list)
    dev_examples: dict[str, list[str]] = defaultdict(list)
    pub_examples_seen: dict[str, set[str]] = defaultdict(set)
    dev_examples_seen: dict[str, set[str]] = defaultdict(set)
    canonical_pub_label_by_key: dict[str, str] = {}
    canonical_dev_label_by_key: dict[str, str] = {}

    with enriched_csv.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            game_name = str(row.get("Name") or "").strip()
            for col in publisher_cols:
                for raw in parse_json_array_cell(row.get(col, "")):
                    if not _wants_company(raw, include_porting_labels=include_porting_labels):
                        continue
                    key = company_key(raw)
                    if not key:
                        continue
                    label = canonical_pub_label_by_key.get(key)
                    if not label:
                        label = normalize_company_name(raw)
                        if not label:
                            continue
                        canonical_pub_label_by_key[key] = label
                    pub_counts[label] += 1
                    if (
                        game_name
                        and game_name not in pub_examples_seen[label]
                        and len(pub_examples[label]) < max(0, int(max_examples))
                    ):
                        pub_examples_seen[label].add(game_name)
                        pub_examples[label].append(game_name)
            for col in developer_cols:
                for raw in parse_json_array_cell(row.get(col, "")):
                    if not _wants_company(raw, include_porting_labels=include_porting_labels):
                        continue
                    key = company_key(raw)
                    if not key:
                        continue
                    label = canonical_dev_label_by_key.get(key)
                    if not label:
                        label = normalize_company_name(raw)
                        if not label:
                            continue
                        canonical_dev_label_by_key[key] = label
                    dev_counts[label] += 1
                    if (
                        game_name
                        and game_name not in dev_examples_seen[label]
                        and len(dev_examples[label]) < max(0, int(max_examples))
                    ):
                        dev_examples_seen[label].add(game_name)
                        dev_examples[label].append(game_name)

    if int(min_count) > 1:
        pub_counts = Counter({k: v for k, v in pub_counts.items() if v >= int(min_count)})
        dev_counts = Counter({k: v for k, v in dev_counts.items() if v >= int(min_count)})

    existing_path = base_yaml if (base_yaml is not None) else out_yaml
    existing = (
        _load_existing_yaml(existing_path)
        if keep_existing
        else {"publishers": {}, "developers": {}}
    )
    pubs_existing_raw: dict[str, Any] = dict(existing.get("publishers", {}))
    devs_existing_raw: dict[str, Any] = dict(existing.get("developers", {}))

    def _existing_tiers(m: dict[str, Any]) -> dict[str, str]:
        out: dict[str, str] = {}
        for label, v in m.items():
            tier = _tier_str(v)
            if not tier:
                continue
            key = company_key(label)
            if not key:
                continue
            out[key] = tier
        return out

    pubs_existing_tier_by_key = _existing_tiers(pubs_existing_raw)
    devs_existing_tier_by_key = _existing_tiers(devs_existing_raw)

    pubs_out: dict[str, Any] = {}
    for label, count in pub_counts.most_common():
        tier_prev = pubs_existing_tier_by_key.get(company_key(label), "")
        if only_missing:
            if tier_prev:
                continue
            pubs_out[label] = {
                "tier": tier_prev,
                "count": int(count),
                "examples": pub_examples.get(label, []),
            }
        else:
            if tier_prev:
                pubs_out[label] = tier_prev

    devs_out: dict[str, Any] = {}
    for label, count in dev_counts.most_common():
        tier_prev = devs_existing_tier_by_key.get(company_key(label), "")
        if only_missing:
            if tier_prev:
                continue
            devs_out[label] = {
                "tier": tier_prev,
                "count": int(count),
                "examples": dev_examples.get(label, []),
            }
        else:
            if tier_prev:
                devs_out[label] = tier_prev

    out_yaml.parent.mkdir(parents=True, exist_ok=True)
    dumper = _FlowListDumper if only_missing else yaml.SafeDumper
    out_yaml.write_text(
        yaml.dump(
            {"publishers": pubs_out, "developers": devs_out},
            Dumper=dumper,
            sort_keys=False,
            allow_unicode=True,
            width=100,
        ),
        encoding="utf-8",
    )

    return CollectProductionTiersResult(
        publishers_total=len(pubs_out),
        developers_total=len(devs_out),
    )
