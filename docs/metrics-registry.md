# Metrics registry + JSONL (how it works)

This project uses **canonical dotted metric keys** internally (typed values), and renders
**user-facing CSV columns** via a configurable **metrics registry**.

## Key concepts

### Metric keys (internal)

Providers and derived logic emit metrics keyed by stable dotted names, e.g.:

- `rawg.genres` (list of strings)
- `steam.store_type` (string)
- `igdb.relationships.dlcs` (list of strings)
- `steamspy.popularity.tags_top` (list of `[tag, count]`)
- `derived.replayability.score_100` (int)
- `composite.reach.score_100` (int)

These keys are the canonical “schema” for extracted data.

### Stable row envelope (internal JSONL)

Internal JSONL files store one row per line using a stable envelope:

```json
{
  "row_id": "rid:…",
  "personal": {"Name": "Doom", "Rating": "5"},
  "pins": {"IGDB_ID": "…", "Steam_AppID": "…"},
  "metrics": {"rawg.genres": ["Action", "Shooter"], "steam.release_year": 2016},
  "diagnostics": {"ReviewTags": "…", "MatchConfidence": "…"},
  "meta": {}
}
```

The code treats JSONL as **typed** (lists/dicts stay lists/dicts). It does **not** parse JSON back
from CSV cells.

## Metrics registry

The registry maps metric keys → CSV column names + a simple type:

- Default: `data/metrics-registry.example.yaml`
- User override: `data/metrics-registry.yaml`
- Full inventory: `docs/available-metrics.md`

Each entry looks like:

```yaml
metrics:
  rawg.genres: { column: RAWG_Genres, type: json }
  steam.release_year: { column: Steam_ReleaseYear, type: int }
  derived.replayability.score_100: { column: Replayability_100, type: int }
```

Supported types: `string|int|float|bool|json|list_csv`

Notes:
- `json` means the JSONL value is expected to be a `list` or `dict` (typed).
- `bool` renders as `YES` / empty in CSV exports.
- CSV rendering is a presentation concern: simple lists become comma-joined; nested lists/dicts are
  rendered as JSON.

### Selection behavior

The registry is also a *column selection mechanism*:

- If a metric key is not in the registry, it is omitted from JSONL exports unless `--all-metrics`
  is enabled.
- Personal fields and pins are always preserved.

## Manifests (for round-trippable exports)

Each JSONL file written by `enrich` has a sidecar manifest:

- `data/output/jsonl/*.manifest.json`

It preserves:
- the exact column order used for CSV export,
- the mapping `CSV column -> metric key` for that JSONL.

The `export` command uses the manifest to render consistent views.

## Where files live

- Catalog JSONL (created by `import`): `data/input/jsonl/Games_Catalog.jsonl`
- Provider/enriched JSONL (created by `enrich`): `data/output/jsonl/Provider_<provider>.jsonl`,
  `data/output/jsonl/Games_Enriched.jsonl`
- User-facing views (created by `export`): `data/output/csv/*`, `data/output/json/*`

## CLI flags

- `import --no-jsonl`: do not write `data/input/jsonl/Games_Catalog.jsonl`
- `enrich --no-jsonl`: memory-only enrich (no JSONL read/write)
- `enrich --reuse-jsonl`: reuse existing provider JSONLs under `data/output/jsonl/` to skip provider
  calls when possible
  - If reuse fails with a “numbers stored as strings” error, delete the referenced
    `data/output/jsonl/Provider_<provider>.jsonl` (and its `.manifest.json`) and rerun without
    `--reuse-jsonl` once to regenerate.

## Remaining work (optional future cleanup)

None currently. The registry is the source of truth for JSONL/export, and derived metrics emit
dotted metric keys directly.
