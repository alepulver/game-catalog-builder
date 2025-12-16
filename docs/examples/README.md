# Provider JSON examples

This folder contains captured provider responses used to understand what each API/library returns.

Generate (or refresh) examples for a game name:

```bash
python -m game_catalog_builder.fetch_provider_examples "Doom (2016)"
```

Outputs are written to `docs/examples/<slug>/` (for example `docs/examples/doom-2016/`).
