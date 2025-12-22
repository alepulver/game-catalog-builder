from __future__ import annotations


def test_wikidata_search_prefers_video_game_candidate(monkeypatch, tmp_path):
    from game_catalog_builder.clients.wikidata_client import WikidataClient

    client = WikidataClient(cache_path=tmp_path / "wikidata_cache.json", min_interval_s=0.0)

    def fake_search(_query: str):
        # Same label, different descriptions.
        return [
            {"id": "QFILM", "label": "X-Men Origins: Wolverine", "description": "2009 film"},
            {"id": "QGAME", "label": "X-Men Origins: Wolverine", "description": "2009 video game"},
        ]

    def fake_get_by_id(qid: str):
        if qid == "QGAME":
            return {"Wikidata_QID": "QGAME", "Wikidata_InstanceOf": "video game"}
        if qid == "QFILM":
            return {"Wikidata_QID": "QFILM", "Wikidata_InstanceOf": "film"}
        return None

    monkeypatch.setattr(client, "_search", fake_search)
    monkeypatch.setattr(client, "get_by_id", fake_get_by_id)
    monkeypatch.setattr(client, "get_by_ids", lambda qids: {q: fake_get_by_id(q) for q in qids})

    got = client.search("X-Men Origins: Wolverine", year_hint=2009)
    assert got is not None
    assert got["Wikidata_QID"] == "QGAME"


def test_wikidata_search_prefers_no_extra_tokens_when_suspicious(monkeypatch, tmp_path):
    from game_catalog_builder.clients import wikidata_client as mod
    from game_catalog_builder.clients.wikidata_client import WikidataClient

    client = WikidataClient(cache_path=tmp_path / "wikidata_cache.json", min_interval_s=0.0)

    def fake_search(_query: str):
        # Ambiguous base title: first candidate is a longer subtitle/edition.
        return [
            {"id": "QSUB", "label": "Worms 2: Armageddon", "description": "video game"},
            {"id": "QBASE", "label": "Worms 2", "description": "video game"},
        ]

    def fake_get_by_id(qid: str):
        if qid in {"QSUB", "QBASE"}:
            return {"Wikidata_QID": qid, "Wikidata_InstanceOf": "video game"}
        return None

    def fake_pick_best_match(_query, candidates, **_kwargs):
        candidates = list(candidates)
        if len(candidates) == 1:
            c = candidates[0]
            return c, 100, [(c.get("label", ""), 100)]
        c = candidates[0]
        top = [(it.get("label", ""), 81 if i == 0 else 79) for i, it in enumerate(candidates)]
        return c, 81, top

    monkeypatch.setattr(client, "_search", fake_search)
    monkeypatch.setattr(client, "get_by_id", fake_get_by_id)
    monkeypatch.setattr(client, "get_by_ids", lambda qids: {q: fake_get_by_id(q) for q in qids})
    monkeypatch.setattr(mod, "pick_best_match", fake_pick_best_match)

    got = client.search("Worms 2", year_hint=None)
    assert got is not None
    assert got["Wikidata_QID"] == "QBASE"
