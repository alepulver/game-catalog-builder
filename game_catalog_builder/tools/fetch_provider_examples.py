import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from game_catalog_builder.clients.hltb_client import HLTBClient
from game_catalog_builder.clients.igdb_client import IGDBClient
from game_catalog_builder.clients.rawg_client import RAWGClient
from game_catalog_builder.clients.steam_client import SteamClient
from game_catalog_builder.clients.steamspy_client import SteamSpyClient


def _slugify(s: str) -> str:
    s = str(s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "example"


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


@dataclass(frozen=True)
class Paths:
    project_root: Path

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def cache_dir(self) -> Path:
        return self.data_dir / "cache"

    @property
    def docs_examples_dir(self) -> Path:
        return self.project_root / "docs" / "examples"


def fetch_examples(
    game_name: str,
    *,
    credentials_path: Path,
    cache_dir: Path,
    out_dir: Path,
) -> None:
    # IGDB (expanded single-call example is preserved in the output request example)
    igdb = IGDBClient(credentials_path=credentials_path, cache_path=cache_dir / "igdb_cache.json")
    igdb_search = igdb.search(game_name)
    _write_json(out_dir / "igdb_search.json", igdb_search)
    if igdb_search and isinstance(igdb_search, list) and igdb_search[0].get("id"):
        igdb_id = int(igdb_search[0]["id"])
        igdb_details = igdb.get_game_details(igdb_id)
        _write_json(out_dir / "igdb_game_details.json", igdb_details)

    rawg = RAWGClient(credentials_path=credentials_path, cache_path=cache_dir / "rawg_cache.json")
    rawg_search = rawg.search(game_name)
    _write_json(out_dir / "rawg_search.json", rawg_search)

    steam = SteamClient(cache_path=cache_dir / "steam_cache.json")
    steam_search = steam.search_appid(game_name)
    _write_json(out_dir / "steam_storesearch.json", steam_search)
    if steam_search and steam_search.get("Steam_AppID"):
        appid = int(steam_search["Steam_AppID"])
        steam_details = steam.get_appdetails(appid)
        _write_json(out_dir / "steam_appdetails.json", steam_details)

        steamspy = SteamSpyClient(cache_path=cache_dir / "steamspy_cache.json")
        steamspy_details = steamspy.get_appdetails(appid)
        _write_json(out_dir / "steamspy_appdetails.json", steamspy_details)

    hltb = HLTBClient(cache_path=cache_dir / "hltb_cache.json")
    hltb_search = hltb.search(game_name)
    _write_json(out_dir / "hltb_search.json", hltb_search)


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(description="Fetch provider JSON examples into docs/examples/")
    parser.add_argument("name", type=str, help="Game name to fetch (e.g., 'Doom (2016)')")
    parser.add_argument(
        "--credentials",
        type=Path,
        default=Path("data/credentials.yaml"),
        help="Credentials YAML (default: data/credentials.yaml)",
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=Path("data/cache"),
        help="Cache directory (default: data/cache)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output directory (default: docs/examples/<slug>/)",
    )
    args = parser.parse_args(argv)

    root = Path(__file__).resolve().parent.parent.parent
    paths = Paths(project_root=root)
    out_dir = args.out or (paths.docs_examples_dir / _slugify(args.name))
    _ensure_dir(out_dir)

    fetch_examples(
        args.name,
        credentials_path=args.credentials,
        cache_dir=args.cache,
        out_dir=out_dir,
    )
    print(f"Wrote examples to: {out_dir}")


if __name__ == "__main__":
    main()

