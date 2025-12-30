from __future__ import annotations

import json
import math
import re
from typing import Any


def to_csv_cell(value: Any) -> str:
    """
    Convert a JSON-serializable value into a CSV cell string.

    Rules are intentionally simple:
    - None/empty -> ""
    - bool -> YES/""
    - list/dict -> JSON string (so it round-trips without lossy comma joins)
    - everything else -> str(value)
    """
    try:
        import pandas as pd

        if pd.isna(value):
            return ""
    except Exception:
        pass
    if isinstance(value, float) and math.isnan(value):
        return ""
    if isinstance(value, str) and value.strip().casefold() == "nan":
        return ""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "YES" if value else ""
    if isinstance(value, str):
        # Avoid embedded newlines/tabs in CSV cells; keep JSONL as the lossless typed artifact.
        s = value.replace("\r", " ").replace("\n", " ").replace("\t", " ")
        s = re.sub(r"\s{2,}", " ", s).strip()
        return s
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        # Prefer spreadsheet readability for simple lists, but keep nested structures as JSON.
        if any(isinstance(x, (list, dict)) for x in value):
            return json.dumps(value, ensure_ascii=False)
        parts = [str(x).strip() for x in value if str(x).strip()]
        return ", ".join(parts)
    return str(value)
