from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any


def as_str(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def as_int(value: object) -> int | None:
    """
    Strict numeric conversion.

    - Accepts: int, integral float
    - Rejects: bool, strings (even if numeric)
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    return None


def as_float(value: object) -> float | None:
    """
    Strict numeric conversion.

    - Accepts: int, float
    - Rejects: bool, strings (even if numeric)
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def year_from_iso_date(value: object) -> int | None:
    """
    Extract YYYY from 'YYYY-MM-DD' or any string containing a 4-digit year.
    """
    s = as_str(value)
    if len(s) >= 4 and s[:4].isdigit():
        y = int(s[:4])
        if 1900 <= y <= 2100:
            return y
    m = re.search(r"\b(19\d{2}|20\d{2})\b", s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def year_from_epoch_seconds(value: object) -> int | None:
    """
    Extract a year from a unix epoch timestamp in seconds.

    Accepts only numeric types (int/integral float). Rejects strings.
    """
    ts = as_int(value)
    if ts is None:
        return None
    if ts <= 0:
        return None
    try:
        return int(datetime.fromtimestamp(ts, tz=timezone.utc).year)
    except Exception:
        return None


def parse_int_text(value: object) -> int | None:
    """
    Parse an integer from provider text fields.

    Use this only when the provider is known to return numeric values as strings.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
        try:
            return int(s)
        except Exception:
            return None
    return None


def parse_float_text(value: object) -> float | None:
    """
    Parse a float from provider text fields.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def normalize_str_list(values: object) -> list[str]:
    """
    Normalize a list-ish value into a de-duped list of non-empty strings.

    Accepts only real lists; returns [] for anything else.
    """
    if not isinstance(values, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for v in values:
        s = as_str(v)
        if not s:
            continue
        k = s.casefold()
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    return out


def get_list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [v for v in value if isinstance(v, dict)]
