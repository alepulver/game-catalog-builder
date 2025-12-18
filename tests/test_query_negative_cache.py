from __future__ import annotations


def test_igdb_caches_empty_query_response(tmp_path, monkeypatch):
    from game_catalog_builder.clients.igdb_client import IGDBClient

    calls = {"post": 0}

    def fake_post(endpoint, query):
        assert endpoint == "games"
        calls["post"] += 1
        return []

    monkeypatch.setattr(
        "game_catalog_builder.clients.igdb_client.IGDBClient._ensure_token",
        lambda self: None,
    )

    client = IGDBClient(
        client_id="x",
        client_secret="y",
        cache_path=tmp_path / "igdb_cache.json",
        language="en",
        min_interval_s=0.0,
    )
    client._token = "t"
    monkeypatch.setattr(client, "_post", fake_post)

    assert client.search("No Such Game") is None
    assert client.search("No Such Game") is None
    assert calls["post"] == 1


def test_hltb_caches_empty_query_response(tmp_path, monkeypatch):
    from game_catalog_builder.clients.hltb_client import HLTBClient

    calls = {"search": 0}

    def fake_search(q):
        calls["search"] += 1
        return []

    client = HLTBClient(cache_path=tmp_path / "hltb_cache.json")
    monkeypatch.setattr(client.client, "search", fake_search)

    assert client.search("No Such Game") is None
    assert client.search("No Such Game") is None
    assert calls["search"] == 1
