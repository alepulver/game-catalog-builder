from __future__ import annotations

import pandas as pd


class _RAWGStub:
    def __init__(self, *, released: str, platforms: list[str], genres: list[str]):
        self._released = released
        self._platforms = platforms
        self._genres = genres

    def get_by_id(self, _id: str):
        return {
            "released": self._released,
            "platforms": [{"platform": {"name": p}} for p in self._platforms],
            "genres": [{"name": g} for g in self._genres],
        }


class _IGDBStub:
    def __init__(self, *, year: str, platforms: str, genres: str):
        self._year = year
        self._platforms = platforms
        self._genres = genres

    def get_by_id(self, _id: str):
        return {
            "IGDB_Year": self._year,
            "IGDB_Platforms": self._platforms,
            "IGDB_Genres": self._genres,
        }


class _HLTBStub:
    def __init__(self, *, year: str, platforms: str):
        self._year = year
        self._platforms = platforms

    def get_by_id(self, _id: str):
        return {"HLTB_ReleaseYear": self._year, "HLTB_Platforms": self._platforms}


def test_fill_eval_tags_flags_year_disagree_hltb_when_rawg_igdb_agree() -> None:
    from game_catalog_builder.cli import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Doom",
                "RAWG_ID": "1",
                "IGDB_ID": "2",
                "HLTB_ID": "3",
                "RAWG_MatchedName": "DOOM",
                "RAWG_MatchScore": "100",
                "IGDB_MatchedName": "Doom",
                "IGDB_MatchScore": "100",
                "HLTB_MatchedName": "DOOM",
                "HLTB_MatchScore": "100",
            }
        ]
    )

    out = fill_eval_tags(
        df,
        sources={"rawg", "igdb", "hltb"},
        clients={
            "rawg": _RAWGStub(released="1993-12-10", platforms=["PC"], genres=["Shooter"]),
            "igdb": _IGDBStub(year="1993", platforms="PC", genres="Shooter"),
            "hltb": _HLTBStub(year="2016", platforms="PC"),
        },
    )
    tags = out.iloc[0]["ReviewTags"]
    assert "year_disagree_hltb" in tags
    assert out.iloc[0]["MatchConfidence"] == "LOW"


def test_fill_eval_tags_flags_platform_disagree_hltb() -> None:
    from game_catalog_builder.cli import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Doom",
                "RAWG_ID": "1",
                "IGDB_ID": "2",
                "HLTB_ID": "3",
                "RAWG_MatchedName": "DOOM",
                "RAWG_MatchScore": "100",
                "IGDB_MatchedName": "Doom",
                "IGDB_MatchScore": "100",
                "HLTB_MatchedName": "DOOM",
                "HLTB_MatchScore": "100",
            }
        ]
    )

    out = fill_eval_tags(
        df,
        sources={"rawg", "igdb", "hltb"},
        clients={
            "rawg": _RAWGStub(released="1993-12-10", platforms=["PC"], genres=["Shooter"]),
            "igdb": _IGDBStub(year="1993", platforms="PC", genres="Shooter"),
            "hltb": _HLTBStub(year="1993", platforms="PlayStation 2"),
        },
    )
    tags = out.iloc[0]["ReviewTags"]
    assert "platform_disagree_hltb" in tags
    assert out.iloc[0]["MatchConfidence"] == "LOW"


def test_fill_eval_tags_flags_genre_disagree_rawg_igdb() -> None:
    from game_catalog_builder.cli import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Doom",
                "RAWG_ID": "1",
                "IGDB_ID": "2",
                "RAWG_MatchedName": "DOOM",
                "RAWG_MatchScore": "100",
                "IGDB_MatchedName": "Doom",
                "IGDB_MatchScore": "100",
            }
        ]
    )

    out = fill_eval_tags(
        df,
        sources={"rawg", "igdb"},
        clients={
            "rawg": _RAWGStub(released="1993-12-10", platforms=["PC"], genres=["Shooter"]),
            "igdb": _IGDBStub(year="1993", platforms="PC", genres="Role-playing (RPG)"),
        },
    )
    tags = out.iloc[0]["ReviewTags"]
    assert "genre_disagree" in tags
    assert out.iloc[0]["MatchConfidence"] == "MEDIUM"
