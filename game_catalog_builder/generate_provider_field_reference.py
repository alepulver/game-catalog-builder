from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml


@dataclass(frozen=True)
class ObservedExample:
    types: tuple[str, ...]
    example: str
    file: str


def _type_name(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, (int, float)):
        return "number"
    if isinstance(v, str):
        return "string"
    if isinstance(v, dict):
        return "object"
    if isinstance(v, list):
        return "array"
    return type(v).__name__


_SEG_RE = re.compile(r"^(?P<key>[^\[]+)(?:\[(?P<idx>\d+)\]|(?P<wild>\[\]))?$")


def _segments(path: str) -> list[tuple[str, str | None]]:
    p = (path or "").strip()
    if p == "$":
        return []
    if p.startswith("$."):
        p = p[2:]
    if p.startswith("$"):
        p = p[1:]
    p = p.lstrip(".")
    if not p:
        return []
    out: list[tuple[str, str | None]] = []
    for raw in p.split("."):
        m = _SEG_RE.match(raw)
        if not m:
            out.append((raw, None))
        else:
            key = m.group("key")
            if m.group("wild"):
                out.append((key, "*"))
            else:
                out.append((key, m.group("idx")))  # idx: None or digit
    return out


def _extract(obj: Any, path: str) -> list[Any]:
    """
    Minimal JSON-path extractor supporting:
    - $.a.b.c
    - a[].b (wildcard list)
    - a[0].b (indexed list)
    - $[] root list wildcard
    """
    p = (path or "").strip()
    if p in ("$", ""):
        return [obj]

    # Special-case root list wildcard: $[]....
    if p.startswith("$[]"):
        rest = p[3:]
        if not isinstance(obj, list):
            return []
        vals: list[Any] = []
        for item in obj:
            vals.extend(_extract(item, "$" + rest))
        return vals

    segs = _segments(p)
    vals: list[Any] = [obj]
    for key, idx in segs:
        next_vals: list[Any] = []
        for v in vals:
            if not isinstance(v, dict):
                continue
            if key not in v:
                continue
            vv = v.get(key)
            if idx is None:
                next_vals.append(vv)
                continue
            # wildcard list
            if idx == "*":
                if isinstance(vv, list):
                    next_vals.extend(vv)
                continue
            # indexed list
            if isinstance(vv, list):
                try:
                    i = int(idx)
                except Exception:
                    continue
                if 0 <= i < len(vv):
                    next_vals.append(vv[i])
        vals = next_vals
        if not vals:
            break
    return vals


def _compact_example(values: list[Any], max_items: int = 5, max_len: int = 180) -> str:
    if not values:
        return ""
    vals = values[:max_items]
    # Prefer scalars for examples.
    scalars = [v for v in vals if not isinstance(v, (dict, list))]
    example_val: Any
    if scalars:
        example_val = scalars[0]
    else:
        example_val = vals[0]
    s = json.dumps(example_val, ensure_ascii=False)
    if len(s) > max_len:
        s = s[: max_len - 3] + "..."
    return s


def _walk_all_paths(v: Any, prefix: str, acc: dict[str, set[str]], examples: dict[str, str]) -> None:
    """
    Record all JSON paths in a document (best-effort), using [] for arrays.
    """
    key = prefix or "$"
    acc.setdefault(key, set()).add(_type_name(v))
    if key not in examples:
        examples[key] = _compact_example([v], max_items=1)

    if isinstance(v, dict):
        for k, vv in v.items():
            p = f"{prefix}.{k}" if prefix else str(k)
            _walk_all_paths(vv, p, acc, examples)
        return

    if isinstance(v, list):
        p = f"{prefix}[]" if prefix else "$[]"
        for item in v:
            _walk_all_paths(item, p, acc, examples)
        return


def _collect_observed_paths_from_obj(obj: Any) -> tuple[dict[str, set[str]], dict[str, str]]:
    acc: dict[str, set[str]] = {}
    ex: dict[str, str] = {}
    _walk_all_paths(obj, "", acc, ex)
    return acc, ex


def _find_example_files(examples_root: Path, filename: str) -> list[Path]:
    return sorted(examples_root.rglob(filename))


def _find_steam_appdetails_data(obj: Any) -> list[Any]:
    """
    steam.appdetails.json is keyed by appid string; return the first entry's data wrapper.
    """
    if not isinstance(obj, dict) or not obj:
        return []
    for k in obj.keys():
        entry = obj.get(k)
        if isinstance(entry, dict):
            return [obj]
    return []


def observe_from_examples(
    examples_root: Path,
    file_hints: list[str],
    path: str,
) -> Optional[ObservedExample]:
    for hint in file_hints:
        for p in _find_example_files(examples_root, hint):
            try:
                obj = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue

            # Support placeholders in the catalog for steam appdetails wrapper shape.
            effective_path = path
            if "<appid>" in effective_path:
                # replace <appid> with the first key in the file
                if isinstance(obj, dict) and obj:
                    first_key = next(iter(obj.keys()))
                    effective_path = effective_path.replace("<appid>", str(first_key))
                else:
                    continue

            values = _extract(obj, effective_path)
            if not values:
                continue

            types = sorted({_type_name(v) for v in values})
            return ObservedExample(
                types=tuple(types),
                example=_compact_example(values),
                file=str(p.relative_to(examples_root)),
            )
    return None


def generate_markdown(catalog: dict[str, Any], examples_root: Path) -> str:
    lines: list[str] = []
    lines.append("# Provider field reference")
    lines.append("")
    lines.append("This reference is generated from `docs/provider-field-catalog.yaml` and enriched with observed examples from `docs/examples/` when available.")
    lines.append("")
    lines.append("Legend:")
    lines.append("- **Observed types/example**: derived from example JSON captures; may be empty if not present in current examples.")
    lines.append("- **Doc links**: point to provider documentation; some providers (Steam Store API) are not formally specified.")
    lines.append("")

    providers = catalog.get("providers") or {}
    for prov_key, prov in providers.items():
        name = prov.get("name") or prov_key
        lines.append(f"## {name}")
        lines.append("")
        docs = prov.get("docs") or []
        if docs:
            lines.append("Docs:")
            for d in docs:
                title = d.get("title") or d.get("url") or ""
                url = d.get("url") or ""
                if url:
                    lines.append(f"- {title}: {url}")
            lines.append("")

        endpoints = prov.get("endpoints") or {}
        for ep_key, ep in endpoints.items():
            lines.append(f"### {ep.get('title') or ep_key}")
            lines.append("")
            file_hints = ep.get("example_file_hints") or []
            fields = ep.get("fields") or []

            # Collect observed paths across example files for this endpoint (discovery).
            observed_types: dict[str, set[str]] = {}
            observed_examples: dict[str, str] = {}
            observed_files: dict[str, str] = {}
            for hint in file_hints:
                for p in _find_example_files(examples_root, hint):
                    try:
                        obj = json.loads(p.read_text(encoding="utf-8"))
                    except Exception:
                        continue
                    acc, ex_map = _collect_observed_paths_from_obj(obj)
                    for path, ts in acc.items():
                        observed_types.setdefault(path, set()).update(ts)
                        observed_examples.setdefault(path, ex_map.get(path, ""))
                        observed_files.setdefault(path, str(p.relative_to(examples_root)))

            lines.append("| Path | Description | Observed types | Example | Example file |")
            lines.append("|---|---|---|---|---|")
            for f in fields:
                path = str(f.get("path") or "")
                desc = str(f.get("description") or "")
                obs = observe_from_examples(examples_root, list(file_hints), path)
                types = ", ".join(obs.types) if obs else ""
                ex = obs.example if obs else ""
                ex_file = obs.file if obs else ""
                lines.append(f"| `{path}` | {desc} | `{types}` | `{ex}` | `{ex_file}` |")
            lines.append("")

            # Observed-but-not-cataloged fields (from examples), for discovery.
            catalog_paths = {str(f.get('path') or '') for f in fields}
            extras = [p for p in observed_types.keys() if p not in catalog_paths]
            if extras:
                lines.append("#### Observed in examples (not yet described)")
                lines.append("")
                lines.append("| Path | Observed types | Example | Example file |")
                lines.append("|---|---|---|---|")
                for p in sorted(extras):
                    types = ", ".join(sorted(observed_types.get(p, set())))
                    ex = observed_examples.get(p, "")
                    ex_file = observed_files.get(p, "")
                    lines.append(f"| `{p}` | `{types}` | `{ex}` | `{ex_file}` |")
                lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    catalog_path = repo_root / "docs" / "provider-field-catalog.yaml"
    examples_root = repo_root / "docs" / "examples"
    out_path = repo_root / "docs" / "provider-field-reference.md"

    if not catalog_path.exists():
        raise SystemExit(f"catalog not found: {catalog_path}")
    catalog = yaml.safe_load(catalog_path.read_text(encoding="utf-8")) or {}
    md = generate_markdown(catalog, examples_root)
    out_path.write_text(md, encoding="utf-8")
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
