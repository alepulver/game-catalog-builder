# Output field configuration (pros/cons)

This project currently has a fixed set of output columns per provider and a fixed `Games_Final.csv` merge. We discussed multiple approaches to make the final output configurable.

The intent is to support:
- selecting which provider columns appear in the final CSV (and ordering them),
- optionally exposing additional provider fields with minimal work,
- keeping provider clients simple and stable.

## Approach 1: “Column selection only” (select extracted columns)

**Idea**
- Keep provider clients as-is (they produce a set of extracted columns).
- A YAML profile selects which columns from those existing outputs go to `Games_Final.csv`.

**Pros**
- Smallest change set; low risk.
- Works immediately with existing caches/outputs.
- No new parsing logic for raw payloads.

**Cons**
- You can only select fields we already extract in code.
- Adding a new provider field still requires code changes in that provider client.

**Best when**
- You mainly want to hide/show/reorder columns.

## Approach 2: “Path-based direct fields” (YAML maps JSON paths → output columns)

**Idea**
- Provider caches store raw payloads by provider ID (or can be re-fetched).
- YAML defines output columns as JSON paths into raw payloads (plus a small transform vocabulary).

**Pros**
- Very flexible: add fields without touching provider code (as long as raw is available).
- Output schema is owned by configuration (good for experimentation).

**Cons**
- Requires implementing and maintaining a path language + transforms.
- Harder to validate correctness (paths can silently break).
- Planning extra calls becomes complex (e.g., RAWG detail vs search result).
- IGDB is query-driven: to use a field, we must request it in the `fields` clause; YAML must influence query construction.

**Best when**
- You want to iterate rapidly on “which fields to include” and can tolerate config complexity.

## Approach 3: “Provider registry of direct fields + derived fields” (YAML selects keys)

**Idea**
- Each provider defines two registries in code:
  - `direct_fields`: thin extraction from provider payloads (paths + minimal normalization).
  - `derived_fields`: computed metrics and heuristics (more complex logic).
- YAML only selects from these named fields/sets:
  - e.g. `igdb:direct:summary`, `steam:direct:metacritic.score`, `validation:derived:ReviewTitle`.
- Providers can also define named sets like `direct:default`, `direct:compact`.

**Pros**
- YAML stays simple (selection only, low repetition with sets).
- Provider logic stays authoritative (fewer fragile config paths).
- Adding a new field is explicit and testable in code.
- Enables a clean future “derived metrics” layer without mixing it into YAML.

**Cons**
- Adding a new field still requires code changes (but localized and testable).
- Needs a small amount of registry plumbing (listing keys, applying selections).

**Best when**
- You want configurability without turning the config into a mini programming language.

## Approach 4: Hybrid (registry + optional path overrides)

**Idea**
- Use Approach 3 as the primary path.
- Allow an “escape hatch” for experimental path-based fields (Approach 2) for a subset of columns.

**Pros**
- Keeps the main workflow robust.
- Still allows experimentation without code changes.

**Cons**
- Two mechanisms to document and support.
- Risk of config sprawl if used too often.

## Other alternatives considered

### Use a full query language (JMESPath/JSONPath)

**Pros**
- Standard semantics, lots of expressiveness.

**Cons**
- Adds dependencies and complexity; error modes can be subtle.
- Still requires IGDB query planning and raw caching decisions.

### Use a schema library (Pydantic/dataclasses) for provider responses

**Pros**
- Strong typing, validation, easier refactors.

**Cons**
- Large upfront cost; provider schemas are huge and change over time.
- Doesn’t solve the “select output columns” problem directly.

## Recommendation

For this project’s scale and iteration style:
- Start with **Approach 3 (registry)** for a maintainable “select fields” mechanism.
- Optionally add a limited **Approach 2 escape hatch** later if you find yourself frequently needing fields that are “obviously direct” but not worth coding yet.

