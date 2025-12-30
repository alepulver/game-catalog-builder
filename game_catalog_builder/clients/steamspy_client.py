from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import requests

from ..config import RETRY, STEAMSPY
from ..utils.utilities import (
    CacheIOTracker,
    RateLimiter,
)
from .http_client import ConfiguredHTTPJSONClient, HTTPJSONClient, HTTPRequestDefaults
from .parse import as_str, normalize_str_list, parse_int_text

STEAMSPY_URL = "https://steamspy.com/api.php"


class SteamSpyClient:
    def __init__(
        self,
        cache_path: str | Path,
        min_interval_s: float = STEAMSPY.min_interval_s,
    ):
        self._session = requests.Session()
        base_http = HTTPJSONClient(self._session, stats=None)
        self.cache_path = Path(cache_path)
        self.stats: dict[str, int] = {
            "by_id_hit": 0,
            "by_id_fetch": 0,
            "by_id_negative_hit": 0,
            "by_id_negative_fetch": 0,
            # HTTP request counters (attempts, including retries).
            "http_get": 0,
        }
        base_http.stats = self.stats
        self._cache_io = CacheIOTracker(self.stats)
        raw = self._cache_io.load_json(self.cache_path)
        if not raw:
            self.cache = {}
        else:
            if not isinstance(raw, dict) or not isinstance(raw.get("by_id"), dict):
                raise ValueError(
                    f"SteamSpy cache file has an unsupported format: {self.cache_path} "
                    "(delete it to rebuild)."
                )
            self.cache = raw.get("by_id") or {}
        self.ratelimiter = RateLimiter(min_interval_s=min_interval_s)
        self._http = ConfiguredHTTPJSONClient(
            base_http,
            HTTPRequestDefaults(
                ratelimiter=self.ratelimiter,
                retries=RETRY.retries,
                counter_key="http_get",
                context_prefix="SteamSpy",
            ),
        )

    def _save_cache(self) -> None:
        self._cache_io.save_json({"by_id": self.cache}, self.cache_path)

    # -------------------------------------------------
    # Main query
    # -------------------------------------------------
    def fetch(self, appid: int) -> dict[str, Any] | None:
        key = str(appid)

        if key in self.cache:
            cached = self.cache[key]
            if not isinstance(cached, dict):
                self.stats["by_id_negative_hit"] += 1
                return None
            self.stats["by_id_hit"] += 1
            return self._extract_metrics(cached)
        data = self._http.get_json(
            STEAMSPY_URL,
            params={
                "request": "appdetails",
                "appid": appid,
            },
            context=f"appdetails appid={appid}",
            on_fail_return=None,
        )
        if not data or not isinstance(data, dict):
            self.cache[key] = None
            self._save_cache()
            self.stats["by_id_negative_fetch"] += 1
            return None

        # SteamSpy sometimes returns {"error": "..."}
        if "error" in data:
            logging.warning(f"Not found in SteamSpy: AppID {appid}. {data.get('error')}")
            self.cache[key] = None
            self._save_cache()
            self.stats["by_id_negative_fetch"] += 1
            return None

        # Cache the full provider response; extraction is computed on-demand.
        self.cache[key] = data
        self._save_cache()
        self.stats["by_id_fetch"] += 1
        return self._extract_metrics(data)

    @staticmethod
    def _extract_metrics(data: dict[str, Any]) -> dict[str, object]:
        positive = parse_int_text(data.get("positive", None))
        negative = parse_int_text(data.get("negative", None))
        score_100: int | None = None
        if positive is not None and negative is not None:
            denom = int(positive) + int(negative)
            if denom > 0:
                score_100 = int(round((int(positive) / float(denom)) * 100.0))

        price = parse_int_text(data.get("price"))
        initialprice = parse_int_text(data.get("initialprice"))
        discount = parse_int_text(data.get("discount"))
        median_forever = parse_int_text(data.get("median_forever"))
        developer = as_str(data.get("developer"))
        publisher = as_str(data.get("publisher"))

        tags_obj = data.get("tags", None)
        tags: list[str] = []
        tags_top: list[list[object]] = []
        if isinstance(tags_obj, dict):
            items: list[tuple[str, int]] = []
            for k, v in tags_obj.items():
                name = as_str(k)
                if not name:
                    continue
                count = parse_int_text(v)
                if count is None:
                    continue
                if count <= 0:
                    continue
                items.append((name, count))
            items.sort(key=lambda x: (-x[1], x[0].casefold()))
            tags = [name for name, _ in items[:15]]
            tags_top = [[name, count] for name, count in items[:50]]

        owners = as_str(data.get("owners"))
        players = parse_int_text(data.get("players_forever"))
        players_2weeks = parse_int_text(data.get("players_2weeks"))
        ccu = parse_int_text(data.get("ccu"))
        playtime_avg = parse_int_text(data.get("average_forever"))
        playtime_avg_2weeks = parse_int_text(data.get("average_2weeks"))
        playtime_median_2weeks = parse_int_text(data.get("median_2weeks"))

        return {
            "steamspy.owners": owners,
            "steamspy.players": players,
            "steamspy.players_2weeks": players_2weeks,
            "steamspy.ccu": ccu,
            "steamspy.playtime_avg": playtime_avg,
            "steamspy.playtime_avg_2weeks": playtime_avg_2weeks,
            "steamspy.playtime_median_2weeks": playtime_median_2weeks,
            "steamspy.playtime_median": median_forever,
            "steamspy.positive": positive,
            "steamspy.negative": negative,
            "steamspy.score_100": score_100,
            "steamspy.price": price,
            "steamspy.initial_price": initialprice,
            "steamspy.discount_percent": discount,
            "steamspy.developer": developer,
            "steamspy.publisher": publisher,
            "steamspy.popularity.tags": normalize_str_list(tags),
            "steamspy.popularity.tags_top": tags_top,
        }

    def format_cache_stats(self) -> str:
        s = self.stats
        base = (
            f"by_id hit={s['by_id_hit']} fetch={s['by_id_fetch']} "
            f"(neg hit={s['by_id_negative_hit']} fetch={s['by_id_negative_fetch']}), "
            f"{HTTPJSONClient.format_timing(s, key='http_get')}"
        )
        base += f", {CacheIOTracker.format_io(s)}"
        http_429 = int(s.get("http_429", 0) or 0)
        if http_429:
            return (
                base
                + f", 429={http_429} retries={int(s.get('http_429_retries', 0) or 0)}"
                + f" backoff_ms={int(s.get('http_429_backoff_ms', 0) or 0)}"
            )
        return base
