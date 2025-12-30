from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _type_name(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, list):
        return "list"
    if isinstance(v, dict):
        return "dict"
    return type(v).__name__


def _walk(prefix: str, obj: Any, out: dict[str, set[str]]) -> None:
    t = _type_name(obj)
    out.setdefault(prefix or "<root>", set()).add(t)
    if isinstance(obj, dict):
        for k, v in obj.items():
            _walk(f"{prefix}.{k}" if prefix else str(k), v, out)
    elif isinstance(obj, list):
        for _i, v in enumerate(obj[:5]):  # limit
            _walk(f"{prefix}[]", v, out)


def generate_reference(examples_root: Path) -> str:
    lines: list[str] = []
    lines.append("# Provider field reference (auto-generated)\n")
    lines.append("This document is generated from example provider JSON files under `docs/examples/`.\n")
    lines.append("It lists the observed JSON paths and value types. Use provider API docs for semantics.\n")

    for provider_dir in sorted(p for p in examples_root.iterdir() if p.is_dir()):
        lines.append(f"\n## {provider_dir.name}\n")
        agg: dict[str, set[str]] = {}
        for json_file in sorted(provider_dir.rglob("*.json")):
            try:
                obj = _load_json(json_file)
            except Exception:
                continue
            _walk("", obj, agg)
        for path, types in sorted(agg.items()):
            t = ", ".join(sorted(types))
            lines.append(f"- `{path}`: `{t}`")
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(
        description="Generate docs/providers/provider-field-reference.md from docs/examples/"
    )
    parser.add_argument(
        "--examples",
        type=Path,
        default=Path("docs/examples"),
        help="Examples root (default: docs/examples)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("docs/providers/provider-field-reference.md"),
        help="Output markdown file (default: docs/providers/provider-field-reference.md)",
    )
    args = parser.parse_args(argv)

    md = generate_reference(args.examples)
    _write_md(args.out, md)
    print(f"Wrote: {args.out}")


if __name__ == "__main__":
    main()
