from __future__ import annotations

import logging


def log_cache_stats(clients: dict[str, object]) -> None:
    order: list[tuple[str, str]] = [
        ("rawg", "[RAWG]"),
        ("igdb", "[IGDB]"),
        ("steam", "[STEAM]"),
        ("steamspy", "[STEAMSPY]"),
        ("hltb", "[HLTB]"),
        ("wikidata", "[WIKIDATA]"),
    ]

    for prov, label in order:
        client = clients.get(prov)
        if client is None:
            continue
        fmt = getattr(client, "format_cache_stats", None)
        if not callable(fmt):
            continue
        try:
            logging.info(f"{label} Cache stats: {fmt()}")
        except Exception:
            # Avoid failing pipelines because of a stats formatting bug.
            logging.info(f"{label} Cache stats: (unavailable)")
