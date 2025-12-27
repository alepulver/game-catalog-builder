from __future__ import annotations

from dataclasses import dataclass

from .utilities import fuzzy_score


@dataclass(frozen=True)
class ProviderConsensus:
    present: tuple[str, ...]
    majority: tuple[str, ...]
    outliers: tuple[str, ...]
    has_majority: bool

    def tags(self) -> list[str]:
        if not self.present:
            return []
        if not self.has_majority:
            return ["provider_no_consensus"]
        tags: list[str] = []
        if self.majority:
            tags.append("provider_consensus:" + "+".join(self.majority))
        for p in self.outliers:
            tags.append(f"provider_outlier:{p}")
        return tags


@dataclass(frozen=True)
class ValueConsensus:
    """
    Consensus for a scalar value (like a year).

    `value` is the majority value, if any.
    """

    present: tuple[str, ...]
    value: int | None
    has_majority: bool


def _union_find_groups(
    items: dict[str, str],
    *,
    years: dict[str, int] | None,
    title_score_threshold: int,
    year_tolerance: int,
    ignore_year_providers: set[str],
) -> list[set[str]]:
    provs = [p for p, title in items.items() if str(title or "").strip()]
    if len(provs) < 2:
        return []

    parent: dict[str, str] = {p: p for p in provs}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i, a in enumerate(provs):
        for b in provs[i + 1 :]:
            if fuzzy_score(items[a], items[b]) < title_score_threshold:
                continue
            if (
                years
                and a in years
                and b in years
                and a not in ignore_year_providers
                and b not in ignore_year_providers
            ):
                if abs(years[a] - years[b]) > year_tolerance:
                    continue
            union(a, b)

    comps: dict[str, set[str]] = {}
    for p in provs:
        comps.setdefault(find(p), set()).add(p)
    return list(comps.values())


def compute_provider_consensus(
    provider_titles: dict[str, str],
    *,
    years: dict[str, int] | None = None,
    title_score_threshold: int = 90,
    year_tolerance: int = 1,
    ignore_year_providers: set[str] | None = None,
    min_providers: int = 3,
) -> ProviderConsensus | None:
    """
    Determine whether a strict majority of providers agree on identity, and which providers are
    outliers.

    Agreement groups are computed using title similarity and (optionally) year proximity.
    """
    present = tuple(sorted(p for p, t in provider_titles.items() if str(t or "").strip()))
    if len(present) < min_providers:
        return None

    ignore_year_providers = ignore_year_providers or {"steam"}
    groups = _union_find_groups(
        provider_titles,
        years=years,
        title_score_threshold=title_score_threshold,
        year_tolerance=year_tolerance,
        ignore_year_providers=set(ignore_year_providers),
    )
    if not groups:
        return ProviderConsensus(present=present, majority=(), outliers=(), has_majority=False)

    groups.sort(key=lambda s: (-len(s), "+".join(sorted(s))))
    best = groups[0]

    has_majority = len(best) > len(present) / 2
    majority = tuple(sorted(best)) if has_majority else ()
    outliers = tuple(sorted(p for p in present if p not in best)) if has_majority else ()
    return ProviderConsensus(
        present=present, majority=majority, outliers=outliers, has_majority=has_majority
    )


def compute_year_consensus(
    years: dict[str, int],
    *,
    ignore_providers: set[str] | None = None,
    min_providers: int = 2,
) -> ValueConsensus | None:
    """
    Find a strict-majority year consensus among providers.

    Defaults to ignoring Steam because its store release year frequently differs for ports,
    remasters, or re-releases.
    """
    ignore_providers = ignore_providers or {"steam"}
    present = tuple(sorted(p for p in years.keys() if p not in ignore_providers))
    if len(present) < min_providers:
        return None

    counts: dict[int, int] = {}
    for p in present:
        counts[years[p]] = counts.get(years[p], 0) + 1
    best_year, best_count = max(counts.items(), key=lambda kv: kv[1])
    has_majority = best_count > len(present) / 2
    return ValueConsensus(
        present=present,
        value=best_year if has_majority else None,
        has_majority=has_majority,
    )


def year_outlier_tags(
    years: dict[str, int],
    *,
    max_diff: int = 1,
    ignore_providers_for_consensus: set[str] | None = None,
) -> list[str]:
    """
    Produce symmetric `year_outlier:<provider>` tags relative to a strict-majority year
    consensus.
    """
    consensus = compute_year_consensus(
        years,
        ignore_providers=ignore_providers_for_consensus,
        min_providers=2,
    )
    if not consensus:
        return []
    if not consensus.has_majority or consensus.value is None:
        return ["year_no_consensus"]
    out: list[str] = []
    for p, y in years.items():
        if abs(y - consensus.value) > max_diff:
            out.append(f"year_outlier:{p}")
    return out


def platform_outlier_tags(platforms: dict[str, set[str]]) -> list[str]:
    """
    Produce symmetric `platform_outlier:<provider>` tags relative to a strict-majority platform
    consensus (computed per platform bucket).
    """
    present = [p for p, s in platforms.items() if s]
    if len(present) < 2:
        return []

    # Count each platform bucket across providers.
    counts: dict[str, int] = {}
    for p in present:
        for bucket in platforms[p]:
            counts[bucket] = counts.get(bucket, 0) + 1

    consensus = {b for b, c in counts.items() if c > len(present) / 2}
    if not consensus:
        return ["platform_no_consensus"]

    out: list[str] = []
    for p in present:
        if platforms[p].isdisjoint(consensus):
            out.append(f"platform_outlier:{p}")
    return out


def company_disagreement_tags(
    company_sets: dict[str, set[str]],
    *,
    kind: str,
    min_providers: int = 2,
) -> list[str]:
    """
    Compute high-signal developer/publisher disagreement tags, while avoiding false positives
    from "bridge" cases (e.g. one provider lists both studios A+B while others list A and B
    separately).

    Returns:
      - `<kind>_disagree` when there is a disconnected overlap graph.
      - `<kind>_outlier:<provider>` when a strict-majority component exists.

    `kind` should be `developer` or `publisher`.
    """
    from .company import LOW_SIGNAL_COMPANY_KEYS

    cleaned: dict[str, set[str]] = {}
    for p, s in company_sets.items():
        cleaned[p] = {x for x in (s or set()) if x and x not in LOW_SIGNAL_COMPANY_KEYS}

    present = [p for p, s in cleaned.items() if s]
    if len(present) < min_providers:
        return []

    parent: dict[str, str] = {p: p for p in present}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i, a in enumerate(present):
        for b in present[i + 1 :]:
            if not cleaned[a].isdisjoint(cleaned[b]):
                union(a, b)

    comps: dict[str, set[str]] = {}
    for p in present:
        comps.setdefault(find(p), set()).add(p)
    if len(comps) <= 1:
        return []

    groups = list(comps.values())
    groups.sort(key=lambda s: (-len(s), "+".join(sorted(s))))
    best = groups[0]
    has_majority = len(best) > len(present) / 2

    # Only emit disagreement when it's strong enough to be actionable:
    # - always for 2-provider comparisons (no majority possible, but it's a clear split)
    # - for 3+ providers, require a strict-majority component to avoid noisy "regional publisher"
    #   splits, unless you explicitly add those later as separate tags.
    if len(present) >= 3 and not has_majority:
        return []

    out = [f"{kind}_disagree"]
    if has_majority:
        for p in sorted(set(present) - set(best)):
            out.append(f"{kind}_outlier:{p}")
    return out


def actionable_mismatch_tags(
    *,
    provider_consensus: ProviderConsensus | None,
    years: dict[str, int],
    year_tags: list[str],
    platform_tags: list[str],
    ambiguous_year_spread: int = 5,
) -> list[str]:
    """
    Add a small set of high-signal tags that are actionable during review, while keeping the
    underlying mismatch logic symmetric across providers.

    Tags:
      - `likely_wrong:<provider>`: provider is a title outlier AND also a year/platform outlier.
      - `ambiguous_title_year`: titles agree but years split widely (reboot/remaster cases).
    """
    out: list[str] = []
    if provider_consensus and provider_consensus.has_majority:
        year_outliers = {t.split(":", 1)[1] for t in year_tags if t.startswith("year_outlier:")}
        platform_outliers = {
            t.split(":", 1)[1] for t in platform_tags if t.startswith("platform_outlier:")
        }
        for p in sorted(set(provider_consensus.outliers) & (year_outliers | platform_outliers)):
            out.append(f"likely_wrong:{p}")

        # When titles agree (no title outliers) but years diverge widely, it's usually a
        # "same name, different game/edition" scenario (e.g. Doom 1993 vs Doom 2016).
        if not provider_consensus.outliers and len(provider_consensus.present) >= 2:
            present_years = [years[p] for p in provider_consensus.present if p in years]
            if len(set(present_years)) >= 2 and (max(present_years) - min(present_years)) >= (
                ambiguous_year_spread
            ):
                counts: dict[int, int] = {}
                for y in present_years:
                    counts[y] = counts.get(y, 0) + 1
                has_strict_majority = max(counts.values()) > len(present_years) / 2
                if not has_strict_majority:
                    out.append("ambiguous_title_year")

    return out
