from __future__ import annotations

from pathlib import Path


def test_igdb_extracts_steam_appid_from_category_shape(tmp_path: Path) -> None:
    from game_catalog_builder.clients.igdb_client import IGDBClient

    client = IGDBClient(client_id="x", client_secret="y", cache_path=tmp_path / "igdb_cache.json")
    appid = client._steam_appid_from_external_games([{"category": "steam", "uid": "620"}])
    assert appid == "620"


def test_extract_steam_appid_from_rawg_store_url() -> None:
    from game_catalog_builder.utils.cross_refs import extract_steam_appid_from_rawg_stores

    rawg_obj = {
        "stores": [
            {
                "store": {"id": 1, "name": "Steam"},
                "url": "https://store.steampowered.com/app/620/DOOM/",
            },
        ]
    }
    assert extract_steam_appid_from_rawg_stores(rawg_obj) == "620"
