from __future__ import annotations

import json
import re
import unicodedata
from typing import Any

_LEGAL_SUFFIX_RE = re.compile(
    r"""(?ix)
    (?:,?\s+|\s*,\s*)
    (?:
        inc\.?|incorporated|llc|l\.l\.c\.|ltd\.?|limited|corp\.?|corporation|
        co\.?|company|gmbh|s\.a\.|s\.a|s\.r\.l\.|s\.r\.l|s\.p\.a\.|s\.p\.a|
        a/s|a\.s\.|ag|bv|oy|oyj|kg|k\.g\.|pte\.?\s+ltd\.?|
        lda\.?|pty\.?(?:\s+ltd\.?)?|
        co\.,?\s*ltd\.?|co\.,?\s*limited|co\.,?\s*ltd
    )\s*$
    """
)

_TRAILING_PARENS_RE = re.compile(r"\s*\([^)]*\)\s*$")
_SPACE_RE = re.compile(r"\s{2,}")
_KEY_CLEAN_RE = re.compile(r"(?i)[^a-z0-9]+")
_GENERIC_SUFFIX_TOKENS = {
    "games",
    "game",
    "software",
    "studio",
    "studios",
    "interactive",
    "entertainment",
    "publishing",
    "publisher",
    "digital",
    "media",
    "production",
    "productions",
}

# Split multi-company strings (conservative): commas and slashes cover the common cases
# ("Ubisoft Montreal, Massive Entertainment, Ubisoft Shanghai") without breaking legitimate
# single-company names like "Running With Scissors" or "Power and Magic Development".


def iter_company_name_variants(value: str) -> list[str]:
    """
    Return a small set of plausible company-name variants for tier matching:
    - normalized original string
    """
    raw = str(value or "").strip()
    if not raw:
        return []
    n0 = normalize_company_name(raw)
    return [n0] if n0 else []


def normalize_company_name(value: Any) -> str:
    """
    Normalize company/publisher/developer names for matching:
    - strips trailing legal suffixes (Inc., Ltd., LLC, Co., Ltd., etc)
    - strips trailing parentheticals used for porting labels (e.g. "Aspyr (Mac, Linux)")
    - collapses whitespace and trims punctuation

    This preserves original case/punctuation where possible (useful for display and tier keys).
    """
    s = str(value or "").strip()
    if not s or s.casefold() in {"nan", "none", "null"}:
        return ""
    s = _TRAILING_PARENS_RE.sub("", s).strip()
    prev = None
    while prev != s:
        prev = s
        s = _LEGAL_SUFFIX_RE.sub("", s).strip().rstrip(",").strip()
    s = _SPACE_RE.sub(" ", s).strip()
    # Ignore labels that are effectively numeric/garbage (e.g. "2015", "3909", "2.21").
    if not re.search(r"[A-Za-z]", s):
        return ""
    return s


def company_key(value: Any) -> str:
    """
    A stronger key used for cross-provider comparisons:
    - normalized (see `normalize_company_name`)
    - lowercased
    - punctuation collapsed to spaces
    """
    s = normalize_company_name(value)
    if not s:
        return ""
    # Drop trademark/registered symbols before normalization so they don't affect the key.
    s = s.replace("™", "").replace("®", "").replace("℠", "")
    # Fold accents so "Montréal" and "Montreal" compare equal.
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))
    # collapse punctuation to spaces, then compact.
    s = _KEY_CLEAN_RE.sub(" ", s).strip()
    s = _SPACE_RE.sub(" ", s).strip()
    return s.casefold()


def company_keys(value: Any) -> set[str]:
    """
    Produce a small set of comparison keys for cross-provider matching.

    This intentionally includes a "generic-suffix stripped" variant (e.g. "2k games" -> "2k")
    to reduce false disagreements when providers differ only in the trailing qualifier.
    """
    base = company_key(value)
    if not base:
        return set()
    out = {base}
    tokens = base.split()
    if len(tokens) >= 2:
        t = tokens
        while len(t) >= 2 and t[-1] in _GENERIC_SUFFIX_TOKENS:
            t = t[:-1]
            k = " ".join(t).strip()
            if k:
                out.add(k)
    return out


def parse_json_array_cell(value: Any) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    if not raw.startswith("["):
        return []
    try:
        v = json.loads(raw)
    except Exception:
        return []
    if not isinstance(v, list):
        return []
    out: list[str] = []
    for x in v:
        t = str(x or "").strip()
        if t:
            out.append(t)
    return out


def company_set_from_json_array_cell(value: Any) -> set[str]:
    out: set[str] = set()
    for x in parse_json_array_cell(value):
        out.update(company_keys(x))
    return {k for k in out if k}


# Known "porting label" entities that are useful to keep in cache/outputs but are low-signal for
# cross-provider identity validation (they often appear only on one provider/store).
LOW_SIGNAL_COMPANY_KEYS = {
    "feral interactive",
    "aspyr",
    "aspyr media",
}
