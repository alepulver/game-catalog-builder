# Metrics registry + JSON/CSV outputs (proposal)

Goal: keep CSV columns stable for spreadsheet workflows, but internally represent extracted data
as a canonical set of metric keys (JSON-path addressable) so we can:

- decouple extraction from CSV column naming,
- output either CSV (current UX) or JSON (better structure, arrays, nested metadata),
- make it easier to add/remove provider fields without editing multiple pipeline files.

## Motivation (current pain points)

- Provider fields are spread across:
  - provider client `extract_fields(...)` implementations,
  - `schema.PUBLIC_DEFAULT_COLS` (column inventory),
  - merge/reorder logic, and
  - derived metrics (`utils/signals.py`).
- Some values are naturally structured (arrays/objects) but are coerced into CSV-friendly strings.
- Adding a new output field usually requires touching multiple places.

## Design: canonical metric keys + a stable row envelope

### Stable row envelope (JSONL row schema)

Every internal JSONL row should use the same top-level shape so downstream tooling does not depend
on ad-hoc keys:

- `row_id`: stable primary key (string; maps to `RowId` in CSV)
- `personal`: user-owned fields (name/rating/notes/etc; source-of-truth for user edits)
- `pins`: pinned provider IDs / overrides (e.g. Steam AppID, IGDB ID, HLTB query override)
- `metrics`: extracted + derived metrics keyed by canonical metric keys (see below)
- `diagnostics`: optional review/validation outputs (tags/confidence), never treated as source-of-truth
- `meta`: optional run metadata (sources enabled, timestamps, tool version)

This makes schema evolution manageable: metric keys can grow without breaking the overall shape.

### Canonical metric keys

Define stable, internal metric keys:

- Provider direct fields:
  - `steam.store.name`
  - `steam.store.categories`
  - `steamspy.popularity.tags_top` (structured)
  - `rawg.popularity.added_by_status.owned`
  - `igdb.relationships.dlcs` (list)
- Derived / composite fields:
  - `composite.reach.score_100`
  - `composite.now.score_100`
  - `derived.replayability.score_100`
  - `derived.content_type.value`

These keys are:

- stable across code changes,
- namespaced (`steam.*`, `rawg.*`, `composite.*`, etc),
- naturally map to JSON paths.

## Keep CSV column names stable

CSV stays the primary user editing surface.

We introduce a mapping layer:

- metric key → CSV column name (existing columns stay as-is)
- metric key → type/format rules (string/int/bool/json-array)

Example mapping (illustrative):

- `rawg.popularity.added_by_status.owned` → `RAWG_AddedByStatusOwned` (int)
- `steamspy.popularity.tags_top` → `SteamSpy_TagsTop` (json)
- `derived.replayability.score_100` → `Replayability_100` (int-as-string)

## Outputs: internal JSONL + exports

Preferred direction:

- Provider caches store raw payloads (durable truth).
- JSONL is an always-on **internal interchange format** (rebuildable projection).
- CSV / pretty JSON are **export views**.

If a transform changes (e.g. epoch→year logic), JSONL is rebuilt from cache by rerunning `enrich`.

Add export modes to relevant commands (initially `enrich`, later `import`/`validate`/`review` if
useful):

- Always write internal JSONL artifacts under:
  - `data/output/jsonl/Provider_<Provider>.jsonl`
  - `data/output/jsonl/Games_Enriched.jsonl`
- Add an export option (name bikeshed; examples):
  - `--export csv` → writes spreadsheet-friendly CSV under `data/output/csv/`
  - `--export json` → writes a user-friendly JSON file (single file) under `data/output/json/`

Recommended internal JSONL row shape:

```json
{
  "row_id": "123",
  "personal": {"Name": "...", "Rating": "...", "...": "..."},
  "pins": {"Steam_AppID": "620", "IGDB_ID": "1234", "...": "..."},
  "metrics": {
    "steam.store.name": "DOOM",
    "steam.store.categories": ["Single-player", "Steam Achievements"],
    "steamspy.popularity.tags_top": [["FPS", 1234], ["Gore", 500]],
    "rawg.popularity.added_by_status.owned": 12345,
    "derived.replayability.score_100": 80
  }
}
```

Notes:

- Arrays remain arrays (no comma-join / re-splitting).
- “CSV ergonomics” (truncation, join) becomes a rendering concern, not an extraction concern.
- Prefer storing normalized values in JSONL when they are deterministic and do not lose structure:
  - e.g. `*_release_year`, `*_score_100`, fallback totals like RAWG added_total.
  - keep the full raw payload in cache, not in JSONL.

## Implementation plan (incremental, low risk)

### Phase 0: inventory + registry scaffolding (no behavior change)

1. Add `game_catalog_builder/metrics/`:
   - `registry.py`: loads/validates a metrics registry config.
   - `render_csv.py`: converts metric dict → current CSV columns (stable names).
   - `render_jsonl.py`: writes JSONL rows.
2. Add `docs/metrics.md` (or extend `how-it-works.md`) explaining metric keys and formats.

### Phase 1: always-on JSONL output (minimal refactor)

1. Keep the current pipelines largely intact (still build DataFrames).
2. After each provider completes, write:
   - `data/output/jsonl/Provider_<Provider>.jsonl` (one JSON object per row)
3. After merge + derived signals, write:
   - `data/output/jsonl/Games_Enriched.jsonl`
4. Keep CSV generation as an export view:
   - default export remains CSV for now (to preserve UX), but files go into `data/output/csv/`.

This validates the “JSONL as internal format” approach without changing providers/caches.

### Sync workflow (explicit commands)

If JSONL becomes the internal representation, spreadsheet editing should be handled explicitly:

- `export`: render a user-editable CSV view from internal JSONL (choose which columns).
- `sync` (from enriched export): ingest an edited enriched view (CSV or user-friendly JSON) and update only:
  - `personal.*` fields (and optionally `pins.*` if you allow pin edits in spreadsheets),
  - never overwrite `metrics.*` (provider/derived metrics are regenerated by `enrich`).

After applying the sync, regenerate the “catalog view” CSV (the normalized/pinned sheet):

- `data/input/Games_Catalog.csv` becomes a rendered view from internal JSONL (`personal.*` + `pins.*`
  + optional diagnostics columns when requested).

This makes it clear which data is user-owned vs derived.

### Phase 2: provider extraction emits metric keys (optional)

1. Introduce per-provider “metric extractor” that takes raw provider payload and returns:
   - `dict[metric_key, value]`
2. Provider clients can continue caching raw payloads exactly as today.
3. CSV provider outputs can be generated by rendering metrics through the registry.

This reduces drift between “what is cached” and “what is output”.

### Phase 3: internal row model becomes metrics-first (bigger refactor)

1. Replace intermediate provider DataFrame merges with a metrics store keyed by `RowId`.
2. Write JSONL incrementally as the authoritative output.
3. Generate CSV/pretty-JSON exports at the end (or on demand).

This is only worth it if you want to fully standardize on internal JSON structures.

## Open questions / decisions

- Where should the registry live?
  - Put it under `data/metrics-registry.yaml` (recommended; user-editable config, not docs).
- How strict should typing be?
  - Keep it pragmatic: enforce only “scalar vs list vs object” + “CSV render rule”.
  - Registry should avoid becoming a mini DSL; complex logic stays in Python derived-metric code.
- Do we want JSON output for provider-only files too?
  - Yes: provider JSONL should be first-class, and CSV provider outputs become optional exports.

## Why JSONL (not one big JSON)

- Streamable: can write incrementally during long runs.
- Greppable and diff-friendly.
- Easy to load into pandas, jq, or other tools.
