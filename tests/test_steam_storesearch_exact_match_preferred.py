from __future__ import annotations


def test_steam_search_prefers_exact_normalized_match(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient
    from game_catalog_builder.clients import steam_client as steam_mod

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)

    # Preload storesearch results for the exact query term.
    query_key = "l:english|cc:US|term:Diablo"
    client._by_query[query_key] = [
        {"id": 111, "name": "Diablo IV", "type": "app"},
        {"id": 222, "name": "Diablo", "type": "app"},
    ]

    # Preload appdetails for final guards and year checks (no network).
    client._by_id["111"] = {"type": "game", "name": "Diablo IV", "release_date": {"date": "2023"}}
    client._by_id["222"] = {"type": "game", "name": "Diablo", "release_date": {"date": "1996"}}

    # If the exact-match filtering doesn't work, this would pick the first candidate (Diablo IV).
    monkeypatch.setattr(
        steam_mod,
        "pick_best_match",
        lambda _q, candidates, **_kw: (list(candidates)[0], 70, []),
    )

    got = client.search_appid("Diablo", year_hint=None)
    assert got is not None
    assert str(got.get("id")) == "222"

