# Provider JSON examples

This folder contains captured provider responses used to understand what each API/library returns.

Related docs:
- `docs/providers/provider-json-schema.md`: naming conventions and notes per provider
- `docs/providers/provider-field-reference.md`: curated catalog + observed examples (generated)

Generate (or refresh) examples for a game name:

```bash
python -m game_catalog_builder.tools.fetch_provider_examples "Doom (2016)"
```

Outputs are written to `docs/examples/<slug>/` (for example `docs/examples/doom-2016/`).
