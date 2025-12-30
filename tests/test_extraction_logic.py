from __future__ import annotations

import json


def test_rawg_extract_metrics_fixture():
    from game_catalog_builder.clients.rawg_client import RAWGClient

    rawg_obj = {
        "id": 2454,
        "name": "DOOM (2016)",
        "name_original": "DOOM",
        "released": "2016-05-13",
        "website": "https://slayersclub.bethesda.net/",
        "reddit_url": "https://www.reddit.com/r/doom/",
        "metacritic_url": "https://www.metacritic.com/game/pc/doom/",
        "background_image": "https://example.com/bg.jpg",
        "description_raw": "A long description " * 100,
        "genres": [{"name": "Action"}, {"name": "Shooter"}],
        "esrb_rating": {"name": "Mature"},
        "platforms": [{"platform": {"name": "PC"}}, {"platform": {"name": "PlayStation 4"}}],
        "tags": [{"name": "Singleplayer"}, {"name": "Atmospheric"}],
        "developers": [{"name": "id Software"}],
        "publishers": [{"name": "Bethesda Softworks"}],
        "rating": 4.2,
        "ratings_count": 1234,
        "metacritic": 85,
        "added": 999,
        "added_by_status": {
            "owned": 500,
            "playing": 10,
            "beaten": 100,
            "toplay": 250,
            "dropped": 5,
        },
    }

    m = RAWGClient.extract_metrics(rawg_obj)
    assert m["rawg.id"] == "2454"
    assert m["rawg.name"] == "DOOM (2016)"
    assert m["rawg.name_original"] == "DOOM"
    assert m["rawg.released"] == "2016-05-13"
    assert m["rawg.year"] == 2016
    assert m["rawg.website"] == "https://slayersclub.bethesda.net/"
    desc = m["rawg.description_raw"]
    assert isinstance(desc, str)
    assert desc.endswith("…")
    assert m["rawg.reddit_url"] == "https://www.reddit.com/r/doom/"
    assert m["rawg.metacritic_url"] == "https://www.metacritic.com/game/pc/doom/"
    assert m["rawg.background_image"] == "https://example.com/bg.jpg"
    assert m["rawg.genres"] == ["Action", "Shooter"]
    assert m["rawg.esrb"] == "Mature"
    assert m["rawg.platforms"] == ["PC", "PlayStation 4"]
    assert m["rawg.tags"] == ["Singleplayer", "Atmospheric"]
    assert m["rawg.ratings_count"] == 1234
    assert m["rawg.metacritic_100"] == 85
    assert m["rawg.popularity.added_total"] == 999
    assert m["rawg.popularity.added_by_status.owned"] == 500
    assert m["rawg.popularity.added_by_status.playing"] == 10
    assert m["rawg.popularity.added_by_status.beaten"] == 100
    assert m["rawg.popularity.added_by_status.toplay"] == 250
    assert m["rawg.popularity.added_by_status.dropped"] == 5
    assert m["rawg.developers"] == ["id Software"]
    assert m["rawg.publishers"] == ["Bethesda Softworks"]


def test_rawg_negative_caching_avoids_repeat_search(tmp_path, monkeypatch):
    from game_catalog_builder.clients.rawg_client import RAWGClient

    calls = {"rawg": 0}

    def fake_get(_self, url, params=None, timeout=None):
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                calls["rawg"] += 1
                return {"results": []}

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = RAWGClient(
        api_key="x",
        cache_path=tmp_path / "rawg_cache.json",
        language="en",
        min_interval_s=0.0,
    )
    assert client.search("No Such Game") is None
    assert client.search("No Such Game") is None
    # RAWG caches negative search results; second call should not hit the network.
    assert calls["rawg"] == 1


def test_steam_extract_metrics_fixture():
    from game_catalog_builder.clients.steam_client import SteamClient

    details = {
        "type": "game",
        "name": "Example Game",
        "is_free": True,
        "release_date": {"coming_soon": False, "date": "10 May, 2016"},
        "platforms": {"windows": True, "mac": False, "linux": True},
        "categories": [{"description": "Single-player"}, {"description": "Steam Achievements"}],
        "genres": [{"description": "Action"}, {"description": "Shooter"}],
        "recommendations": {"total": 999},
    }

    m = SteamClient.extract_metrics(123, details)
    assert m["steam.app_id"] == "123"
    assert m["steam.name"] == "Example Game"
    url = m["steam.url"]
    assert isinstance(url, str)
    assert url.endswith("/123/")
    assert m["steam.store_type"] == "game"
    assert m["steam.release_year"] == 2016
    assert m["steam.platforms"] == ["Windows", "Linux"]
    assert m["steam.tags"] == ["Action", "Shooter"]
    assert m["steam.review_count"] == 999
    assert m["steam.price"] == "Free"
    assert m["steam.categories"] == ["Single-player", "Steam Achievements"]


def test_steam_details_are_cached_by_appid(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    calls = {"appdetails": 0}

    def fake_get(_self, url, params=None, timeout=None):
        class Resp:
            status_code = 200
            headers: dict[str, str] = {}

            def raise_for_status(self):
                return None

            def json(self):
                if "appdetails" in url:
                    calls["appdetails"] += 1
                    return {"123": {"success": True, "data": {"name": "Example Game", "is_free": True}}}
                raise AssertionError(f"unexpected url {url}")

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    d1 = client.get_app_details(123)
    d2 = client.get_app_details(123)
    assert d1 is not None
    assert d2 is not None
    assert d1["name"] == "Example Game"
    assert d2["name"] == "Example Game"
    assert calls["appdetails"] == 1


def test_steamspy_fetch_extracts_expected_fields(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steamspy_client import SteamSpyClient

    def fake_get(_self, url, params=None, timeout=None):
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "owners": "10,000 .. 20,000",
                    "players_forever": 1234,
                    "ccu": 12,
                    "average_forever": 56,
                    "median_forever": 12,
                    "price": 999,
                    "initialprice": 1999,
                    "discount": 50,
                    "developer": "Example Dev",
                    "publisher": "Example Pub",
                }

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = SteamSpyClient(cache_path=tmp_path / "steamspy_cache.json", min_interval_s=0.0)
    data = client.fetch(999)
    assert data is not None
    assert data == {
        "steamspy.owners": "10,000 .. 20,000",
        "steamspy.players": 1234,
        "steamspy.players_2weeks": None,
        "steamspy.ccu": 12,
        "steamspy.playtime_avg": 56,
        "steamspy.playtime_avg_2weeks": None,
        "steamspy.playtime_median_2weeks": None,
        "steamspy.playtime_median": 12,
        "steamspy.positive": None,
        "steamspy.negative": None,
        "steamspy.score_100": None,
        "steamspy.price": 999,
        "steamspy.initial_price": 1999,
        "steamspy.discount_percent": 50,
        "steamspy.developer": "Example Dev",
        "steamspy.publisher": "Example Pub",
        "steamspy.popularity.tags": [],
        "steamspy.popularity.tags_top": [],
    }

    client._cache_io.flush()
    raw = json.loads((tmp_path / "steamspy_cache.json").read_text(encoding="utf-8"))
    assert raw["by_id"]["999"]["owners"] == "10,000 .. 20,000"


def test_steamspy_extracts_tag_cloud(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steamspy_client import SteamSpyClient

    def fake_get(_self, url, params=None, timeout=None):
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "owners": "10,000 .. 20,000",
                    "tags": {"Roguelike": 123, "Multiplayer": 50, "Zombies": 5},
                }

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = SteamSpyClient(cache_path=tmp_path / "steamspy_cache.json", min_interval_s=0.0)
    data = client.fetch(111)
    assert data is not None
    assert data["steamspy.popularity.tags"][0] == "Roguelike"
    assert data["steamspy.popularity.tags_top"][0][0] == "Roguelike"


def test_igdb_expanded_single_call_extracts_expected_fields(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "game_catalog_builder.clients.igdb_client.IGDBClient._ensure_token", lambda self: None
    )
    from game_catalog_builder.clients.igdb_client import IGDBClient

    calls = []

    def fake_post(endpoint, query):
        calls.append(endpoint)
        assert endpoint == "games"
        return [
            {
                "id": 7351,
                "name": "Doom",
                "first_release_date": 1463097600,
                "summary": "Rip and tear until it is done.",
                "alternative_names": [{"name": "DOOM"}],
                "websites": [
                    {"url": "https://slayersclub.bethesda.net/"},
                    {"url": "https://doom.com/"},
                ],
                "platforms": [{"name": "PC (Microsoft Windows)"}],
                "genres": [{"name": "Shooter"}],
                "themes": [{"name": "Action"}],
                "keywords": [{"name": "gore"}],
                "game_modes": [{"name": "Single player"}],
                "player_perspectives": [{"name": "First person"}],
                "franchises": [{"name": "Doom"}],
                "game_engines": [{"name": "id Tech 6"}],
                "parent_game": {"name": "Doom"},
                "version_parent": {"name": "Doom"},
                "dlcs": [{"name": "Doom - DLC Pack"}],
                "expansions": [{"name": "Doom: Expansion"}],
                "ports": [{"name": "Doom (Switch)"}],
                "involved_companies": [
                    {"company": {"name": "id Software"}, "developer": True, "publisher": False},
                    {
                        "company": {"name": "Bethesda Softworks"},
                        "developer": False,
                        "publisher": True,
                    },
                ],
                "external_games": [{"external_game_source": 1, "uid": "379720"}],
            }
        ]

    client = IGDBClient(
        client_id="x",
        client_secret="y",
        cache_path=tmp_path / "igdb_cache.json",
        language="en",
        min_interval_s=0.0,
    )
    client._token = "t"
    monkeypatch.setattr(client, "_post", fake_post)

    enriched = client.search("Doom (2016)")
    assert enriched is not None
    assert calls == ["games"]
    assert enriched["igdb.id"] == "7351"
    assert enriched["igdb.name"] == "Doom"
    assert enriched["igdb.year"] == 2016
    assert enriched["igdb.summary"] == "Rip and tear until it is done."
    assert "https://slayersclub.bethesda.net/" in enriched["igdb.websites"]
    assert enriched["igdb.alternative_names"] == ["DOOM"]
    assert enriched["igdb.platforms"] == ["PC (Microsoft Windows)"]
    assert enriched["igdb.genres"] == ["Shooter"]
    assert enriched["igdb.themes"] == ["Action"]
    assert enriched["igdb.keywords"] == ["gore"]
    assert enriched["igdb.game_modes"] == ["Single player"]
    assert enriched["igdb.perspectives"] == ["First person"]
    assert enriched["igdb.franchise"] == ["Doom"]
    assert enriched["igdb.engine"] == ["id Tech 6"]
    assert enriched["igdb.relationships.parent_game"] == "Doom"
    assert enriched["igdb.relationships.version_parent"] == "Doom"
    assert enriched["igdb.relationships.dlcs"] == ["Doom - DLC Pack"]
    assert enriched["igdb.relationships.expansions"] == ["Doom: Expansion"]
    assert enriched["igdb.relationships.ports"] == ["Doom (Switch)"]
    assert enriched["igdb.cross_ids.steam_app_id"] == "379720"
    assert enriched["igdb.developers"] == ["id Software"]
    assert enriched["igdb.publishers"] == ["Bethesda Softworks"]


def test_igdb_similarity_threshold_negative_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "game_catalog_builder.clients.igdb_client.IGDBClient._ensure_token", lambda self: None
    )
    from game_catalog_builder.clients.igdb_client import IGDBClient

    calls = {"games": 0}

    def fake_post(endpoint, query):
        calls["games"] += 1
        return [{"id": 1, "name": "Completely Different", "genres": []}]

    client = IGDBClient(
        client_id="x",
        client_secret="y",
        cache_path=tmp_path / "igdb_cache.json",
        language="en",
        min_interval_s=0.0,
    )
    client._token = "t"
    monkeypatch.setattr(client, "_post", fake_post)

    assert client.search("Example Game") is None
    assert client.search("Example Game") is None
    assert calls["games"] == 1


def test_wikidata_extract_metrics_fixture(tmp_path) -> None:
    from game_catalog_builder.clients.wikidata_client import WikidataClient

    client = WikidataClient(cache_path=tmp_path / "wikidata_cache.json", min_interval_s=0.0)
    client._labels = {
        "QDEV": "id Software",
        "QPUB": "GT Interactive",
        "QPLAT": "PC (MS-DOS)",
        "QGENRE": "first-person shooter",
        "QSER": "Doom",
    }

    entity = {
        "id": "Q123",
        "labels": {"en": {"value": "Doom"}},
        "descriptions": {"en": {"value": "1993 video game"}},
        "claims": {
            "P577": [{"mainsnak": {"datavalue": {"value": {"time": "+1993-12-10T00:00:00Z"}}}}],
            "P178": [{"mainsnak": {"datavalue": {"value": {"id": "QDEV"}}}}],
            "P123": [{"mainsnak": {"datavalue": {"value": {"id": "QPUB"}}}}],
            "P400": [{"mainsnak": {"datavalue": {"value": {"id": "QPLAT"}}}}],
            "P136": [{"mainsnak": {"datavalue": {"value": {"id": "QGENRE"}}}}],
            "P179": [{"mainsnak": {"datavalue": {"value": {"id": "QSER"}}}}],
        },
        "sitelinks": {"enwiki": {"title": "Doom (1993 video game)"}},
    }

    m = client._extract_metrics(entity)
    assert m["wikidata.qid"] == "Q123"
    assert m["wikidata.label"] == "Doom"
    assert m["wikidata.description"] == "1993 video game"
    assert m["wikidata.release_year"] == 1993
    assert m["wikidata.developers"] == ["id Software"]
    assert m["wikidata.publishers"] == ["GT Interactive"]
    assert m["wikidata.platforms"] == ["PC (MS-DOS)"]
    assert m["wikidata.series"] == ["Doom"]
    assert m["wikidata.genres"] == ["first-person shooter"]
    wiki = m["wikidata.wikipedia"]
    assert isinstance(wiki, str)
    assert wiki.endswith("/Doom_(1993_video_game)")


def test_wikidata_extract_metrics_falls_back_to_non_en_label(tmp_path) -> None:
    from game_catalog_builder.clients.wikidata_client import WikidataClient

    client = WikidataClient(cache_path=tmp_path / "wikidata_cache.json", min_interval_s=0.0)
    entity = {
        "id": "Q1",
        "labels": {"en-gb": {"value": "Example Label"}},
        "descriptions": {"en-gb": {"value": "Example description"}},
        "claims": {},
        "sitelinks": {},
    }
    m = client._extract_metrics(entity)
    assert m["wikidata.label"] == "Example Label"
    assert m["wikidata.description"] == "Example description"


def test_wikidata_extract_metrics_falls_back_to_enwiki_title(tmp_path) -> None:
    from game_catalog_builder.clients.wikidata_client import WikidataClient

    client = WikidataClient(cache_path=tmp_path / "wikidata_cache.json", min_interval_s=0.0)
    entity = {
        "id": "Q2",
        "labels": {},
        "descriptions": {"en": {"value": "Example description"}},
        "claims": {},
        "sitelinks": {"enwiki": {"title": "Left 4 Dead"}},
    }
    m = client._extract_metrics(entity)
    assert m["wikidata.label"] == "Left 4 Dead"


def test_hltb_caches_by_id_or_name_fallback(tmp_path):
    from game_catalog_builder.clients.hltb_client import HLTBClient

    class FakeResult:
        def __init__(self, game_id, game_name, main_story):
            self.game_id = game_id
            self.game_name = game_name
            self.main_story = main_story
            self.main_extra = ""
            self.completionist = ""
            self.release_world = 2000
            self.profile_platforms = ["PC"]
            self.game_web_link = "https://howlongtobeat.com/game/123"

    class FakeHLTB:
        def __init__(self, results):
            self._results = results
            self.calls = 0

        def search(self, name):
            self.calls += 1
            return self._results

    cache_path = tmp_path / "hltb_cache.json"
    client = HLTBClient(cache_path=cache_path)
    fake = FakeHLTB([FakeResult(123, "Example Game", "10")])
    client.client = fake

    data1 = client.search("Example Game")
    data2 = client.search("Example Game")
    assert data1 is not None
    assert data2 is not None
    assert data1 == data2
    assert data1["hltb.name"] == "Example Game"
    assert fake.calls == 1

    client._cache_io.flush()
    raw = json.loads(cache_path.read_text(encoding="utf-8"))
    assert raw["by_query"]["q:Example Game"][0]["game_id"] == 123
    assert raw["by_id"]["123"]["main_story"] == "10"
    assert raw["by_id"]["123"]["release_world"] == 2000


def test_steam_to_steamspy_pipeline_streaming(tmp_path, monkeypatch):
    import csv

    from game_catalog_builder.metrics.registry import load_metrics_registry
    from game_catalog_builder.pipelines.enrich_pipeline import process_steam_and_steamspy_streaming

    input_csv = tmp_path / "in.csv"
    with input_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["RowId", "Name"])
        w.writeheader()
        w.writerows(
            [
                {"RowId": "rid:1", "Name": "Game A"},
                {"RowId": "rid:2", "Name": "Game B"},
                {"RowId": "rid:3", "Name": "Game C"},
            ]
        )

    steam_out = tmp_path / "Provider_Steam.csv"
    steamspy_out = tmp_path / "Provider_SteamSpy.csv"

    def fake_get(_self, url, params=None, timeout=None):
        def _appids_from_url(u: str) -> list[str]:
            if "appids=" not in u:
                return []
            raw = u.split("appids=", 1)[1].split("&", 1)[0]
            return [s.strip() for s in raw.split(",") if s.strip()]

        class Resp:
            status_code = 200
            headers: dict[str, str] = {}

            def raise_for_status(self):
                return None

            def json(self):
                if "storesearch" in url:
                    term = (params or {}).get("term")
                    if term == "Game A":
                        return {"items": [{"id": 111, "name": "Game A", "type": "app"}]}
                    if term == "Game B":
                        return {"items": [{"id": 222, "name": "Game B", "type": "app"}]}
                    return {"items": []}
                if "appdetails" in url:
                    ids = _appids_from_url(url)
                    payload = {}
                    for appid in ids:
                        payload[appid] = {
                            "success": True,
                            "data": {
                                "steam_appid": int(appid),
                                "name": f"Game {appid}",
                                "type": "game",
                                "is_free": True,
                            },
                        }
                    return payload
                if "steamspy.com" in url:
                    return {
                        "owners": "1 .. 2",
                        "players_forever": 1,
                        "ccu": 1,
                        "average_forever": 1,
                    }
                raise AssertionError(f"unexpected url {url}")

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    (tmp_path / "metrics.yaml").write_text(
        "\n".join(
            [
                "version: 2",
                "metrics:",
                "  steam.app_id: { column: Steam_AppID, type: string }",
                "  steam.name: { column: Steam_Name, type: string }",
                "  steam.url: { column: Steam_URL, type: string }",
                "  steam.website: { column: Steam_Website, type: string }",
                "  steam.short_description: { column: Steam_ShortDescription, type: string }",
                "  steam.store_type: { column: Steam_StoreType, type: string }",
                "  steam.release_year: { column: Steam_ReleaseYear, type: string }",
                "  steam.platforms: { column: Steam_Platforms, type: json }",
                "  steam.tags: { column: Steam_Tags, type: json }",
                "  steam.review_count: { column: Steam_ReviewCount, type: string }",
                "  steam.price: { column: Steam_Price, type: string }",
                "  steam.categories: { column: Steam_Categories, type: json }",
                "  steam.metacritic_100: { column: Steam_Metacritic, type: string }",
                "  steam.developers: { column: Steam_Developers, type: json }",
                "  steam.publishers: { column: Steam_Publishers, type: json }",
                "  steamspy.owners: { column: SteamSpy_Owners, type: string }",
                "  steamspy.players: { column: SteamSpy_Players, type: string }",
                "  steamspy.players_2weeks: { column: SteamSpy_Players2Weeks, type: string }",
                "  steamspy.ccu: { column: SteamSpy_CCU, type: string }",
                "  steamspy.playtime_avg: { column: SteamSpy_PlaytimeAvg, type: string }",
                "  steamspy.playtime_avg_2weeks: { column: SteamSpy_PlaytimeAvg2Weeks, type: string }",
                "  steamspy.playtime_median_2weeks: { column: SteamSpy_PlaytimeMedian2Weeks, type: string }",
                "  steamspy.positive: { column: SteamSpy_Positive, type: string }",
                "  steamspy.negative: { column: SteamSpy_Negative, type: string }",
                "  steamspy.popularity.tags: { column: SteamSpy_Tags, type: json }",
                "  steamspy.popularity.tags_top: { column: SteamSpy_TagsTop, type: string }",
                "  steamspy.score_100: { column: SteamSpy_Score_100, type: string }",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    registry = load_metrics_registry(tmp_path / "metrics.yaml")

    process_steam_and_steamspy_streaming(
        input_csv=input_csv,
        steam_output_csv=steam_out,
        steamspy_output_csv=steamspy_out,
        steam_cache_path=tmp_path / "steam_cache.json",
        steamspy_cache_path=tmp_path / "steamspy_cache.json",
        registry=registry,
    )

    with steam_out.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert [r["Steam_AppID"] for r in rows] == ["111", "222", ""]

    with steamspy_out.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    owners = [r["SteamSpy_Owners"] for r in rows]
    assert owners == ["1 .. 2", "1 .. 2", ""]
