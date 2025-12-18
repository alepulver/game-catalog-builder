from __future__ import annotations

import pandas as pd


class _IGDBStub:
    def __init__(self, steam_appid: str):
        self._steam_appid = steam_appid

    def get_by_id(self, _id: str):
        return {"IGDB_SteamAppID": self._steam_appid}


class _RAWGStub:
    def __init__(self, store_url: str):
        self._store_url = store_url

    def get_by_id(self, _id: str):
        return {"stores": [{"url": self._store_url}]}


def test_fill_eval_tags_flags_steam_appid_disagree_igdb() -> None:
    from game_catalog_builder.cli import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Doom",
                "YearHint": "1993",
                "IGDB_ID": "1",
                "RAWG_ID": "2",
                "Steam_AppID": "620",
                "Steam_MatchedName": "DOOM",
                "Steam_MatchScore": "100",
                "IGDB_MatchedName": "Doom",
                "IGDB_MatchScore": "100",
                "RAWG_MatchedName": "DOOM",
                "RAWG_MatchScore": "100",
                "HLTB_MatchedName": "",
            }
        ]
    )
    out = fill_eval_tags(df, clients={"igdb": _IGDBStub("999")})
    tags = out.iloc[0]["ReviewTags"]
    assert "steam_appid_disagree:igdb" in tags
    assert out.iloc[0]["MatchConfidence"] == "LOW"


def test_fill_eval_tags_flags_steam_appid_disagree_rawg() -> None:
    from game_catalog_builder.cli import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Doom",
                "RAWG_ID": "2",
                "Steam_AppID": "620",
                "Steam_MatchedName": "DOOM",
                "Steam_MatchScore": "100",
                "RAWG_MatchedName": "DOOM",
                "RAWG_MatchScore": "100",
                "HLTB_MatchedName": "",
            }
        ]
    )
    out = fill_eval_tags(df, clients={"rawg": _RAWGStub("https://store.steampowered.com/app/10/")})
    tags = out.iloc[0]["ReviewTags"]
    assert "steam_appid_disagree:rawg" in tags
    assert out.iloc[0]["MatchConfidence"] == "LOW"

