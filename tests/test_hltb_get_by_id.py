from __future__ import annotations


def test_hltb_get_by_id_uses_library_search_from_id(tmp_path, monkeypatch):
    from game_catalog_builder.clients.hltb_client import HLTBClient

    class FakeEntry:
        game_id = 8940
        game_name = "Example Game"
        main_story = "1"
        main_extra = "2"
        completionist = "3"

    client = HLTBClient(cache_path=tmp_path / "hltb_cache.json")

    calls: list[int] = []

    def fake_search_from_id(game_id: int):
        calls.append(game_id)
        return FakeEntry()

    monkeypatch.setattr(client.client, "search_from_id", fake_search_from_id)

    data = client.get_by_id("8940")
    assert data is not None
    assert data["HLTB_ID"] == "8940"
    assert data["HLTB_Name"] == "Example Game"
    assert calls == [8940]

    # Cached on second call.
    data2 = client.get_by_id(8940)
    assert data2 is not None
    assert calls == [8940]
