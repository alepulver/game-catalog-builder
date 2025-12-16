# Provider schemas and fields

- `docs/providers/provider-fields.md`: practical mapping of provider fields → current CSV columns
- `docs/providers/provider-field-catalog.yaml`: curated field catalog (with descriptions and doc links)
- `docs/providers/provider-field-reference.md`: generated reference (catalog + observed examples)
- `docs/providers/provider-json-schema.md`: conventions for captured example JSON files under `docs/examples/`

Generate/refresh the reference:

```bash
python -m game_catalog_builder.generate_provider_field_reference
```

