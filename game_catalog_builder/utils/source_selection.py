from __future__ import annotations


def parse_sources(
    raw: str, *, allowed: set[str], aliases: dict[str, list[str]] | None = None
) -> list[str]:
    """
    Parse a provider list string like:
      - "all"
      - "core"
      - "igdb,rawg,steam"

    Returns a de-duplicated list preserving order.
    """
    s = str(raw or "").strip()
    if not s:
        raise SystemExit("Missing --source value")

    tokens = [t.strip().lower() for t in s.split(",") if t.strip()]
    out: list[str] = []
    seen: set[str] = set()

    def _add(x: str) -> None:
        if x in seen:
            return
        seen.add(x)
        out.append(x)

    if len(tokens) == 1 and tokens[0] in {"all"}:
        for x in sorted(allowed):
            _add(x)
        return out

    aliases = aliases or {}
    for t in tokens:
        if t in aliases:
            for x in aliases[t]:
                if x not in allowed:
                    raise SystemExit(f"Unknown provider in alias '{t}': {x}")
                _add(x)
            continue
        if t not in allowed:
            raise SystemExit(
                f"Unknown provider: {t}. Allowed: {', '.join(sorted(allowed | set(aliases)))}"
            )
        _add(t)
    return out

