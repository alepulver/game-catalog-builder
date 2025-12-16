from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import requests

from game_catalog_builder.clients import HLTBClient, IGDBClient
from game_catalog_builder.utils import fuzzy_score, load_credentials, normalize_game_name


def slugify(value: str) -> str:
    s = (value or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-") or "game"

def extract_year_from_query(game_name: str) -> Optional[int]:
    m = re.search(r"\((\d{4})\)\s*$", (game_name or "").strip())
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def dump_hltb_object(obj: Any) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for k, v in vars(obj).items():
        try:
            json.dumps(v)
            data[k] = v
        except TypeError:
            data[k] = str(v)
    return data


def pick_best_candidate(query: str, candidates: list[Dict[str, Any]], *, name_key: str = "name") -> tuple[Optional[Dict[str, Any]], int]:
    norm_query = normalize_game_name(query)
    best: Optional[Dict[str, Any]] = None
    best_rank: tuple[int, int, int] = (-1, -1, -10**9)

    for candidate in candidates:
        name = str(candidate.get(name_key, "") or "")
        score = fuzzy_score(query, name)
        is_exact = 1 if normalize_game_name(name) == norm_query else 0
        length_rank = -abs(len(name) - len(query))
        rank = (score, is_exact, length_rank)
        if rank > best_rank:
            best_rank = rank
            best = candidate

    return best, best_rank[0]

def pick_best_candidate_with_year(
    query: str,
    candidates: list[Dict[str, Any]],
    *,
    name_key: str = "name",
    year: Optional[int] = None,
    year_from_candidate: Optional[callable] = None,
) -> tuple[Optional[Dict[str, Any]], int]:
    norm_query = normalize_game_name(query)
    best: Optional[Dict[str, Any]] = None
    best_rank: tuple[int, int, int, int] = (-1, -1, -1, -10**9)

    for candidate in candidates:
        name = str(candidate.get(name_key, "") or "")
        score = fuzzy_score(query, name)
        is_exact = 1 if normalize_game_name(name) == norm_query else 0
        length_rank = -abs(len(name) - len(query))

        year_match = 0
        if year and year_from_candidate:
            try:
                cy = year_from_candidate(candidate)
            except Exception:
                cy = None
            if cy == year:
                year_match = 1

        rank = (score, year_match, is_exact, length_rank)
        if rank > best_rank:
            best_rank = rank
            best = candidate

    return best, best_rank[0]


def fetch_rawg(game_name: str, api_key: str, out_dir: Path, *, language: str = "en") -> Optional[Dict[str, Any]]:
    try:
        search_url = "https://api.rawg.io/api/games"
        search_resp = requests.get(
            search_url,
            params={"search": game_name, "page_size": 10, "key": api_key, "lang": language},
            timeout=15,
        )
        search_resp.raise_for_status()
        search_json = search_resp.json()
        write_json(out_dir / "rawg.search.json", search_json)

        year = extract_year_from_query(game_name)
        results: list[Dict[str, Any]] = list(search_json.get("results") or [])

        def _rawg_year(c: Dict[str, Any]) -> Optional[int]:
            released = str(c.get("released") or "")
            if len(released) >= 4 and released[:4].isdigit():
                return int(released[:4])
            return None

        best, score = pick_best_candidate_with_year(
            game_name,
            results,
            name_key="name",
            year=year,
            year_from_candidate=_rawg_year,
        )
        write_json(out_dir / "rawg.best.json", {"score": score, "best": best})

        if not best or not best.get("id"):
            write_json(out_dir / "rawg.detail.error.json", {"error": "No RAWG id selected"})
            return None

        detail_url = f"https://api.rawg.io/api/games/{best['id']}"
        detail_resp = requests.get(detail_url, params={"key": api_key, "lang": language}, timeout=15)
        detail_resp.raise_for_status()
        detail_json = detail_resp.json()
        write_json(out_dir / "rawg.detail.json", detail_json)
        return {"id": best["id"], "name": best.get("name")}
    except Exception as e:
        write_json(out_dir / "rawg.error.json", {"error": str(e)})
        return None


def fetch_steam(game_name: str, out_dir: Path) -> Optional[int]:
    try:
        search_url = "https://store.steampowered.com/api/storesearch"
        fallback_term = re.sub(r"\s*\(.*\)\s*$", "", game_name).strip()
        terms = [t for t in [game_name, fallback_term] if t]
        seen = set()
        terms = [t for t in terms if not (t in seen or seen.add(t))]

        search_json: Dict[str, Any] = {}
        used_term = terms[0]
        all_attempts: list[dict[str, Any]] = []
        for term in terms:
            resp = requests.get(search_url, params={"term": term, "l": "english", "cc": "US"}, timeout=15)
            resp.raise_for_status()
            candidate_json = resp.json()
            all_attempts.append({"term": term, "response": candidate_json})
            if (candidate_json.get("items") or []) and (candidate_json.get("total") or 0) > 0:
                search_json = candidate_json
                used_term = term
                break
            search_json = candidate_json
            used_term = term

        write_json(out_dir / "steam.storesearch.attempts.json", all_attempts)
        write_json(out_dir / "steam.storesearch.meta.json", {"selected_term": used_term, "terms": terms})
        write_json(out_dir / "steam.storesearch.json", search_json)

        year = extract_year_from_query(game_name)
        items: list[Dict[str, Any]] = list(search_json.get("items") or [])

        # Steam search results do not include release year; if a year is provided, inspect appdetails
        # for the best few candidates and prefer the one matching the requested year.
        scored = []
        for item in items:
            name = str(item.get("name") or "")
            scored.append((fuzzy_score(used_term, name), item))
        scored.sort(key=lambda x: x[0], reverse=True)

        best = None
        best_score = -1
        if year and scored:
            for s, item in scored[:8]:
                appid = item.get("id")
                if not appid:
                    continue
                details_resp = requests.get(
                    "https://store.steampowered.com/api/appdetails",
                    params={"appids": int(appid), "l": "english"},
                    timeout=15,
                )
                details_resp.raise_for_status()
                details_json = details_resp.json()
                entry = details_json.get(str(appid)) or {}
                data = entry.get("data") or {}
                date_str = (data.get("release_date") or {}).get("date") or ""
                y = None
                m = re.search(r"(\d{4})", date_str)
                if m:
                    try:
                        y = int(m.group(1))
                    except ValueError:
                        y = None
                if y == year:
                    best = item
                    best_score = s
                    break

        if best is None:
            best, best_score = pick_best_candidate(used_term, items, name_key="name")

        score = best_score
        write_json(out_dir / "steam.best.json", {"score": score, "best": best})

        if not best or not best.get("id"):
            write_json(out_dir / "steam.appdetails.error.json", {"error": "No Steam appid selected"})
            return None

        appid = int(best["id"])
        appdetails_url = "https://store.steampowered.com/api/appdetails"
        details_resp = requests.get(appdetails_url, params={"appids": appid, "l": "english"}, timeout=15)
        details_resp.raise_for_status()
        write_json(out_dir / "steam.appdetails.json", details_resp.json())
        return appid
    except Exception as e:
        write_json(out_dir / "steam.error.json", {"error": str(e)})
        write_json(out_dir / "steam.appdetails.error.json", {"error": str(e)})
        return None


def fetch_steamspy(appid: int, out_dir: Path) -> None:
    try:
        url = "https://steamspy.com/api.php"
        resp = requests.get(url, params={"request": "appdetails", "appid": appid}, timeout=15)
        resp.raise_for_status()
        write_json(out_dir / "steamspy.appdetails.json", resp.json())
    except Exception as e:
        write_json(out_dir / "steamspy.error.json", {"error": str(e)})


def fetch_igdb(game_name: str, credentials: Dict[str, Any], out_dir: Path, *, language: str = "en") -> None:
    try:
        igdb = IGDBClient(
            client_id=credentials.get("igdb", {}).get("client_id", ""),
            client_secret=credentials.get("igdb", {}).get("client_secret", ""),
            cache_path=out_dir / "_igdb_cache_unused.json",
            language=language,
            min_interval_s=0.0,
        )

        # Example: expanded single-call query (no secondary endpoint lookups).
        expanded_query = f'''
        search "{game_name}";
        fields
          id,name,
          genres.name,
          themes.name,
          game_modes.name,
          player_perspectives.name,
          franchises.name,
          game_engines.name,
          external_games.external_game_source,external_games.uid;
        limit 10;
        '''

        query = f'''
        search "{game_name}";
        fields
          id,name,slug,summary,storyline,first_release_date,
          aggregated_rating,aggregated_rating_count,total_rating,total_rating_count,rating,rating_count,
          genres,themes,game_modes,player_perspectives,platforms,franchises,game_engines,keywords,websites,
          involved_companies,collection,dlcs,expansions,remakes,remasters,ports,similar_games,
          external_games.external_game_source,external_games.uid;
        limit 10;
        '''

        expanded_raw = igdb._post("games", expanded_query)  # intentionally raw for docs/examples
        write_json(out_dir / "igdb.games.expanded_single.json", expanded_raw)

        raw = igdb._post("games", query)  # intentionally raw for documentation/examples
        write_json(out_dir / "igdb.games.search.json", raw)

        year = extract_year_from_query(game_name)
        candidates: list[Dict[str, Any]] = list(raw or [])

        # IGDB stores release date as epoch seconds; convert properly for year matching.
        def _igdb_year_epoch(c: Dict[str, Any]) -> Optional[int]:
            ts = c.get("first_release_date")
            if not ts:
                return None
            try:
                import datetime
                return datetime.datetime.fromtimestamp(int(ts), tz=datetime.timezone.utc).year
            except Exception:
                return None

        best, score = pick_best_candidate_with_year(
            game_name,
            candidates,
            name_key="name",
            year=year,
            year_from_candidate=_igdb_year_epoch,
        )
        write_json(out_dir / "igdb.best.json", {"score": score, "best": best})

        if not best:
            write_json(out_dir / "igdb.resolved.error.json", {"error": "No IGDB match selected"})
            return

        def _resolve(endpoint: str, ids: Any, field: str = "name") -> list[str]:
            if not ids:
                return []
            if not isinstance(ids, list):
                return []
            return igdb._resolve_ids(endpoint, ids, field=field)

        resolved = {
            "id": best.get("id"),
            "name": best.get("name"),
            "genres": _resolve("genres", best.get("genres")),
            "themes": _resolve("themes", best.get("themes")),
            "game_modes": _resolve("game_modes", best.get("game_modes")),
            "player_perspectives": _resolve("player_perspectives", best.get("player_perspectives")),
            "platforms": _resolve("platforms", best.get("platforms")),
            "franchises": _resolve("franchises", best.get("franchises")),
            "game_engines": _resolve("game_engines", best.get("game_engines")),
            "keywords": _resolve("keywords", best.get("keywords")),
        }
        steam_appid = ""
        external_games = best.get("external_games", []) or []

        # Record Steam appid only if it is directly present in the IGDB payload.
        # Do not perform additional IGDB calls to resolve external_game ids; Steam selection should
        # remain independent of IGDB.
        for ext in external_games:
            if not isinstance(ext, dict):
                continue
            source = ext.get("external_game_source")
            if source in (1, "1"):  # 1 == Steam (external_game_sources)
                steam_appid = str(ext.get("uid") or "").strip()
                break

        resolved["steam_appid"] = steam_appid
        write_json(out_dir / "igdb.resolved.json", resolved)
    except Exception as e:
        write_json(out_dir / "igdb.error.json", {"error": str(e)})


def fetch_hltb(game_name: str, out_dir: Path) -> None:
    try:
        hltb = HLTBClient(cache_path=out_dir / "_hltb_cache_unused.json")
        year = extract_year_from_query(game_name)
        fallback_term = re.sub(r"\s*\(.*\)\s*$", "", game_name).strip()
        terms = [t for t in [game_name, fallback_term] if t]
        seen = set()
        terms = [t for t in terms if not (t in seen or seen.add(t))]

        attempts: list[dict[str, Any]] = []
        all_results: list[Any] = []
        used_term = terms[0]
        for term in terms:
            results = hltb.client.search(term) or []
            attempts.append({"term": term, "results": [dump_hltb_object(r) for r in results]})
            if results:
                all_results = results
                used_term = term
                break

        write_json(out_dir / "hltb.search.json", {"term": used_term, "attempts": attempts})

        if not all_results:
            write_json(out_dir / "hltb.best.json", None)
            return

        def _hltb_year(obj: Any) -> Optional[int]:
            for attr in ("release_world", "release_na", "release_eu", "release_jp"):
                v = getattr(obj, attr, None)
                if not v:
                    continue
                m = re.search(r"(\d{4})", str(v))
                if m:
                    try:
                        return int(m.group(1))
                    except ValueError:
                        continue
            return None

        best = None
        best_rank: tuple[int, int] = (-1, -1)
        for obj in all_results:
            name = getattr(obj, "game_name", "") or ""
            score = fuzzy_score(used_term, name)
            year_match = 0
            if year and _hltb_year(obj) == year:
                year_match = 1
            rank = (score, year_match)
            if rank > best_rank:
                best_rank = rank
                best = obj

        write_json(out_dir / "hltb.best.json", dump_hltb_object(best))
    except Exception as e:
        write_json(out_dir / "hltb.error.json", {"error": str(e)})
        write_json(out_dir / "hltb.search.json", {"error": str(e)})
        write_json(out_dir / "hltb.best.json", None)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch provider JSON examples into docs/examples/")
    parser.add_argument("game", nargs="?", default="Doom", help="Game name to fetch (default: Doom)")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("docs/examples"),
        help="Output root directory (default: docs/examples)",
    )
    parser.add_argument(
        "--credentials",
        type=Path,
        default=Path("data/credentials.yaml"),
        help="Credentials YAML path (default: data/credentials.yaml)",
    )
    args = parser.parse_args()

    out_dir = args.out / slugify(args.game)
    out_dir.mkdir(parents=True, exist_ok=True)

    credentials = load_credentials(args.credentials)

    rawg_key = credentials.get("rawg", {}).get("api_key", "")
    if rawg_key:
        fetch_rawg(args.game, rawg_key, out_dir, language="en")
    else:
        write_json(out_dir / "rawg.error.json", {"error": "Missing RAWG api_key"})
        write_json(out_dir / "rawg.search.json", None)
        write_json(out_dir / "rawg.best.json", None)
        write_json(out_dir / "rawg.detail.error.json", {"error": "Missing RAWG api_key"})

    appid = fetch_steam(args.game, out_dir)
    if appid:
        fetch_steamspy(appid, out_dir)
    else:
        write_json(out_dir / "steamspy.error.json", {"error": "Missing Steam AppID match"})
        write_json(out_dir / "steamspy.appdetails.json", None)

    igdb_client_id = credentials.get("igdb", {}).get("client_id", "")
    igdb_client_secret = credentials.get("igdb", {}).get("client_secret", "")
    if igdb_client_id and igdb_client_secret:
        fetch_igdb(args.game, credentials, out_dir, language="en")
    else:
        write_json(out_dir / "igdb.error.json", {"error": "Missing IGDB client_id/client_secret"})
        write_json(out_dir / "igdb.games.search.json", None)
        write_json(out_dir / "igdb.best.json", None)
        write_json(out_dir / "igdb.resolved.error.json", {"error": "Missing IGDB client_id/client_secret"})

    fetch_hltb(args.game, out_dir)


if __name__ == "__main__":
    main()
