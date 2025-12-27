from __future__ import annotations

import re


def extract_steam_appid_from_rawg_stores(rawg_obj: object) -> str:
    if not isinstance(rawg_obj, dict):
        return ""
    stores = rawg_obj.get("stores")
    if not isinstance(stores, list):
        return ""
    for it in stores:
        if not isinstance(it, dict):
            continue
        url = str(it.get("url") or "").strip()
        if not url:
            continue
        m = re.search(r"/app/(\d+)\b", url)
        if m:
            return m.group(1)
    return ""
