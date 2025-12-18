from __future__ import annotations


def test_hltb_strips_trailing_year_parentheses_for_search(tmp_path, monkeypatch):
    from game_catalog_builder.clients.hltb_client import HLTBClient

    class FakeResult:
        game_id = 123
        game_name = "Spider-Man 2"
        main_story = "10"
        main_extra = "12"
        completionist = "15"

    calls: list[str] = []

    def fake_search(q: str):
        calls.append(q)
        if q == "Spider-Man 2 (2004)":
            return []
        if q == "Spider-Man 2":
            return [FakeResult()]
        return []

    client = HLTBClient(cache_path=tmp_path / "hltb_cache.json")
    monkeypatch.setattr(client.client, "search", fake_search)

    out = client.search("Spider-Man 2 (2004)")
    assert out is not None
    assert out["HLTB_Name"] == "Spider-Man 2"
    assert calls == ["Spider-Man 2 (2004)", "Spider-Man 2"]

    # Original key should be cached as a hit (so we don't keep searching).
    out2 = client.search("Spider-Man 2 (2004)")
    assert out2 is not None
    assert calls == ["Spider-Man 2 (2004)", "Spider-Man 2"]

