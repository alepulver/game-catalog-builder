from __future__ import annotations

from pathlib import Path

from ..clients import (
    HLTBClient,
    IGDBClient,
    RAWGClient,
    SteamClient,
    SteamSpyClient,
    WikidataClient,
)
from ..config import IGDB, RAWG, STEAM, STEAMSPY, WIKIDATA


def build_provider_clients(
    *, sources: set[str], credentials: dict[str, object], cache_dir: Path
) -> dict[str, object]:
    """
    Instantiate provider clients from a source set, credentials, and a cache directory.
    """
    clients: dict[str, object] = {}

    if "rawg" in sources:
        api_key = str((credentials.get("rawg", {}) or {}).get("api_key", "") or "").strip()
        if api_key:
            clients["rawg"] = RAWGClient(
                api_key=api_key,
                cache_path=cache_dir / "rawg_cache.json",
                min_interval_s=RAWG.min_interval_s,
            )

    if "igdb" in sources:
        client_id = str((credentials.get("igdb", {}) or {}).get("client_id", "") or "").strip()
        secret = str((credentials.get("igdb", {}) or {}).get("client_secret", "") or "").strip()
        if client_id and secret:
            clients["igdb"] = IGDBClient(
                client_id=client_id,
                client_secret=secret,
                cache_path=cache_dir / "igdb_cache.json",
                min_interval_s=IGDB.min_interval_s,
            )

    if "steam" in sources:
        clients["steam"] = SteamClient(
            cache_path=cache_dir / "steam_cache.json",
            min_interval_s=STEAM.storesearch_min_interval_s,
        )

    if "steamspy" in sources:
        clients["steamspy"] = SteamSpyClient(
            cache_path=cache_dir / "steamspy_cache.json",
            min_interval_s=STEAMSPY.min_interval_s,
        )

    if "wikidata" in sources:
        clients["wikidata"] = WikidataClient(
            cache_path=cache_dir / "wikidata_cache.json",
            min_interval_s=WIKIDATA.min_interval_s,
        )

    if "hltb" in sources:
        clients["hltb"] = HLTBClient(cache_path=cache_dir / "hltb_cache.json")

    return clients
