from __future__ import annotations


def test_hltb_query_variants_include_lower_upper_and_punct_strip() -> None:
    from game_catalog_builder.clients.hltb_client import HLTBClient

    c = HLTBClient(cache_path=":memory:")
    v = c._query_variants("Blake Stone: Planet Strike!")
    assert v[0] == "Blake Stone: Planet Strike!"
    assert "Blake Stone: Planet Strike" in v
    assert "Blake Stone" in v
    assert "blake stone: planet strike!" not in v
    assert "BLAKE STONE: PLANET STRIKE!" not in v


def test_hltb_query_variants_simplify_full_hd() -> None:
    from game_catalog_builder.clients.hltb_client import HLTBClient

    c = HLTBClient(cache_path=":memory:")
    v = c._query_variants("Galaxy on Fire 2 Full HD")
    assert "Galaxy on Fire 2 HD" in v


def test_hltb_query_variants_roman_numerals_to_digits() -> None:
    from game_catalog_builder.clients.hltb_client import HLTBClient

    c = HLTBClient(cache_path=":memory:")
    v = c._query_variants("Unreal Tournament III")
    assert "Unreal Tournament 3" in v


def test_hltb_search_falls_back_to_case_variants_when_no_results(tmp_path) -> None:
    from game_catalog_builder.clients.hltb_client import HLTBClient

    class _StubHLTB:
        def __init__(self):
            self.calls: list[str] = []

        def search(self, q: str):
            self.calls.append(q)
            if q == "Amid Evil":
                return []
            if q == "amid evil":

                class _R:
                    game_id = 123
                    game_name = "AMID EVIL"

                return [_R()]
            return []

        def search_from_id(self, _id: int):
            return None

    c = HLTBClient(cache_path=tmp_path / "hltb_cache.json")
    c.client = _StubHLTB()

    out = c.search("Amid Evil")
    assert out is not None
    assert out["hltb.name"] == "AMID EVIL"
    assert "Amid Evil" in c.client.calls
    assert "amid evil" in c.client.calls
