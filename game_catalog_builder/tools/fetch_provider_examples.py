from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from game_catalog_builder.clients.hltb_client import HLTBClient
from game_catalog_builder.clients.igdb_client import IGDBClient
from game_catalog_builder.clients.rawg_client import RAWGClient
from game_catalog_builder.clients.steam_client import SteamClient
from game_catalog_builder.clients.steamspy_client import SteamSpyClient
from game_catalog_builder.utils.utilities import load_credentials


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
    creds = load_credentials(credentials_path)

    # IGDB (expanded single-call request lives inside IGDBClient.search/get_by_id)
    igdb_creds = creds.get("igdb")
    if isinstance(igdb_creds, dict):
        client_id = str(igdb_creds.get("client_id") or "").strip()
        client_secret = str(igdb_creds.get("client_secret") or "").strip()
    else:
        client_id = ""
        client_secret = ""
    if client_id and client_secret:
        igdb = IGDBClient(
            client_id=client_id,
            client_secret=client_secret,
            cache_path=cache_dir / "igdb_cache.json",
            language="en",
        )
        igdb_search = igdb.search(game_name)
        _write_json(out_dir / "igdb_search.json", igdb_search)
        igdb_id_raw = igdb_search.get("igdb.id") if isinstance(igdb_search, dict) else None
        if igdb_id_raw and str(igdb_id_raw).isdigit():
            igdb_details = igdb.get_by_id(int(str(igdb_id_raw)))
            _write_json(out_dir / "igdb_details.json", igdb_details)
    else:
        _write_json(out_dir / "igdb_search.json", {"error": "missing igdb credentials"})

    # RAWG
    rawg_creds = creds.get("rawg")
    api_key = str(rawg_creds.get("api_key") or "").strip() if isinstance(rawg_creds, dict) else ""
    if api_key:
        rawg = RAWGClient(
            api_key=api_key,
            cache_path=cache_dir / "rawg_cache.json",
            language="en",
        )
        rawg_search = rawg.search(game_name)
        _write_json(out_dir / "rawg_search.json", rawg_search)
        rawg_id_raw = rawg_search.get("rawg.id") if isinstance(rawg_search, dict) else None
        if rawg_id_raw and str(rawg_id_raw).isdigit():
            rawg_details = rawg.get_by_id(int(str(rawg_id_raw)))
            _write_json(out_dir / "rawg_details.json", rawg_details)
    else:
        _write_json(out_dir / "rawg_search.json", {"error": "missing rawg api_key"})

    steam = SteamClient(cache_path=cache_dir / "steam_cache.json")
    steam_search = steam.search_appid(game_name)
    _write_json(out_dir / "steam_storesearch.json", steam_search)
    if steam_search and isinstance(steam_search, dict) and str(steam_search.get("id") or "").isdigit():
        appid = int(str(steam_search.get("id")))
        steam_details = steam.get_app_details(appid)
        _write_json(out_dir / "steam_appdetails.json", steam_details)

        steamspy = SteamSpyClient(cache_path=cache_dir / "steamspy_cache.json")
        steamspy_details = steamspy.fetch(appid)
        _write_json(out_dir / "steamspy_appdetails.json", steamspy_details)

    hltb = HLTBClient(cache_path=cache_dir / "hltb_cache.json")
    hltb_search = hltb.search(game_name)
    _write_json(out_dir / "hltb_search.json", hltb_search)


def main(argv: Optional[list[str]] = None) -> None:  # noqa: UP045
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
