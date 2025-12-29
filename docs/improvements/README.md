# Improvement proposals

This folder contains design notes for potential future refactors and features. These documents are intentionally exploratory (pros/cons, tradeoffs, and alternatives) so we can revisit them later.

- `output-field-configuration.md`: approaches for configuring which provider fields land in final outputs.
- `provider-unused-fields.md`: inventory of cached provider fields not currently surfaced (high-ROI candidates).
- `metrics-registry-and-json-output.md`: proposal for canonical metric keys + optional JSONL output (while keeping CSV stable).
- `tried-and-rejected.md`: experiments we reverted (avoid re-trying without new evidence).
