from __future__ import annotations

import json


def test_rawg_extract_fields_fixture():
    from game_catalog_builder.clients.rawg_client import RAWGClient

    rawg_obj = {
        "id": 2454,
        "name": "DOOM (2016)",
        "released": "2016-05-13",
        "genres": [{"name": "Action"}, {"name": "Shooter"}],
        "platforms": [{"platform": {"name": "PC"}}, {"platform": {"name": "PlayStation 4"}}],
        "tags": [{"name": "Singleplayer"}, {"name": "Atmospheric"}],
        "rating": 4.2,
        "ratings_count": 1234,
        "metacritic": 85,
    }

    fields = RAWGClient.extract_fields(rawg_obj)
    assert fields["RAWG_ID"] == "2454"
    assert fields["RAWG_Name"] == "DOOM (2016)"
    assert fields["RAWG_Released"] == "2016-05-13"
    assert fields["RAWG_Year"] == "2016"
    assert fields["RAWG_Genre"] == "Action"
    assert fields["RAWG_Genre2"] == "Shooter"
    assert fields["RAWG_Platforms"] == "PC, PlayStation 4"
    assert fields["RAWG_Tags"] == "Singleplayer, Atmospheric"
    assert fields["RAWG_Rating"] == "4.2"
    assert fields["RAWG_RatingsCount"] == "1234"
    assert fields["RAWG_Metacritic"] == "85"


def test_rawg_negative_caching_avoids_repeat_search(tmp_path, monkeypatch):
    from game_catalog_builder.clients.rawg_client import RAWGClient

    calls = {"rawg": 0}

    def fake_get(url, params=None, timeout=None):
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                calls["rawg"] += 1
                return {"results": []}

        return Resp()

    monkeypatch.setattr("requests.get", fake_get)

    client = RAWGClient(
        api_key="x",
        cache_path=tmp_path / "rawg_cache.json",
        language="en",
        min_interval_s=0.0,
    )
    assert client.search("No Such Game") is None
    assert client.search("No Such Game") is None
    assert calls["rawg"] == 1


def test_steam_extract_fields_fixture():
    from game_catalog_builder.clients.steam_client import SteamClient

    details = {
        "name": "Example Game",
        "is_free": True,
        "release_date": {"coming_soon": False, "date": "10 May, 2016"},
        "platforms": {"windows": True, "mac": False, "linux": True},
        "categories": [{"description": "Single-player"}, {"description": "Steam Achievements"}],
        "genres": [{"description": "Action"}, {"description": "Shooter"}],
        "recommendations": {"total": 999},
    }

    fields = SteamClient.extract_fields(123, details)
    assert fields["Steam_AppID"] == "123"
    assert fields["Steam_Name"] == "Example Game"
    assert fields["Steam_ReleaseYear"] == "2016"
    assert fields["Steam_Platforms"] == "Windows, Linux"
    assert fields["Steam_Tags"] == "Action, Shooter"
    assert fields["Steam_ReviewCount"] == "999"
    assert fields["Steam_Price"] == "Free"
    assert fields["Steam_Categories"] == "Single-player, Steam Achievements"


def test_steam_details_are_cached_by_appid(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    calls = {"appdetails": 0}

    def fake_get(url, params=None, timeout=None):
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                if "appdetails" in url:
                    calls["appdetails"] += 1
                    return {
                        "123": {"success": True, "data": {"name": "Example Game", "is_free": True}}
                    }
                raise AssertionError(f"unexpected url {url}")

        return Resp()

    monkeypatch.setattr("requests.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    d1 = client.get_app_details(123)
    d2 = client.get_app_details(123)
    assert d1["name"] == "Example Game"
    assert d2["name"] == "Example Game"
    assert calls["appdetails"] == 1


def test_steamspy_fetch_extracts_expected_fields(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steamspy_client import SteamSpyClient

    def fake_get(url, params=None, timeout=None):
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "owners": "10,000 .. 20,000",
                    "players_forever": 1234,
                    "ccu": 12,
                    "average_forever": 56,
                }

        return Resp()

    monkeypatch.setattr("requests.get", fake_get)

    client = SteamSpyClient(cache_path=tmp_path / "steamspy_cache.json", min_interval_s=0.0)
    data = client.fetch(999)
    assert data == {
        "SteamSpy_Owners": "10,000 .. 20,000",
        "SteamSpy_Players": "1234",
        "SteamSpy_CCU": "12",
        "SteamSpy_PlaytimeAvg": "56",
    }

    raw = json.loads((tmp_path / "steamspy_cache.json").read_text(encoding="utf-8"))
    assert raw.get("999") == data


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
                "platforms": [{"name": "PC (Microsoft Windows)"}],
                "genres": [{"name": "Shooter"}],
                "themes": [{"name": "Action"}],
                "game_modes": [{"name": "Single player"}],
                "player_perspectives": [{"name": "First person"}],
                "franchises": [{"name": "Doom"}],
                "game_engines": [{"name": "id Tech 6"}],
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
    assert calls == ["games"]
    assert enriched["IGDB_ID"] == "7351"
    assert enriched["IGDB_Name"] == "Doom"
    assert enriched["IGDB_Year"] == "2016"
    assert enriched["IGDB_Platforms"] == "PC (Microsoft Windows)"
    assert enriched["IGDB_Genres"] == "Shooter"
    assert enriched["IGDB_Themes"] == "Action"
    assert enriched["IGDB_GameModes"] == "Single player"
    assert enriched["IGDB_Perspectives"] == "First person"
    assert enriched["IGDB_Franchise"] == "Doom"
    assert enriched["IGDB_Engine"] == "id Tech 6"
    assert enriched["IGDB_SteamAppID"] == "379720"


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


def test_hltb_caches_by_id_or_name_fallback(tmp_path):
    from game_catalog_builder.clients.hltb_client import HLTBClient

    class FakeResult:
        def __init__(self, game_id, game_name, main_story):
            self.game_id = game_id
            self.game_name = game_name
            self.main_story = main_story
            self.main_extra = ""
            self.completionist = ""

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
    assert data1 == data2
    assert data1["HLTB_Name"] == "Example Game"
    assert fake.calls == 1

    raw = json.loads(cache_path.read_text(encoding="utf-8"))
    assert raw["by_name"]["example game"] == "123"
    assert raw["by_id"]["123"]["HLTB_Main"] == "10"


def test_steam_to_steamspy_pipeline_streaming(tmp_path, monkeypatch):
    import csv

    from game_catalog_builder.cli import process_steam_and_steamspy_streaming

    input_csv = tmp_path / "in.csv"
    with input_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Name"])
        w.writeheader()
        w.writerows([{"Name": "Game A"}, {"Name": "Game B"}, {"Name": "Game C"}])

    steam_out = tmp_path / "Provider_Steam.csv"
    steamspy_out = tmp_path / "Provider_SteamSpy.csv"

    def fake_get(url, params=None, timeout=None):
        class Resp:
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
                    appid = str((params or {}).get("appids"))
                    return {
                        appid: {"success": True, "data": {"name": f"Game {appid}", "is_free": True}}
                    }
                if "steamspy.com" in url:
                    return {
                        "owners": "1 .. 2",
                        "players_forever": 1,
                        "ccu": 1,
                        "average_forever": 1,
                    }
                raise AssertionError(f"unexpected url {url}")

        return Resp()

    monkeypatch.setattr("requests.get", fake_get)

    process_steam_and_steamspy_streaming(
        input_csv=input_csv,
        steam_output_csv=steam_out,
        steamspy_output_csv=steamspy_out,
        steam_cache_path=tmp_path / "steam_cache.json",
        steamspy_cache_path=tmp_path / "steamspy_cache.json",
    )

    with steam_out.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert [r["Steam_AppID"] for r in rows] == ["111", "222", ""]

    with steamspy_out.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    owners = [r["SteamSpy_Owners"] for r in rows]
    assert owners == ["1 .. 2", "1 .. 2", ""]
