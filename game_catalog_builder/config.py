from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetryConfig:
    retries: int = 3
    base_sleep_s: float = 1.0
    jitter_s: float = 0.3
    http_429_default_retry_after_s: float = 5.0


@dataclass(frozen=True)
class MatchingConfig:
    min_score: int = 65
    suspicious_score: int = 90
    suggestions_limit: int = 10


@dataclass(frozen=True)
class ValidationConfig:
    title_score_warn: int = 90
    year_max_diff: int = 1


@dataclass(frozen=True)
class RequestConfig:
    timeout_s: int = 10


@dataclass(frozen=True)
class SteamConfig:
    storesearch_min_interval_s: float = 0.5
    appdetails_min_interval_s: float = 1.2
    appdetails_batch_size: int = 25
    appdetails_refine_candidates: int = 10


@dataclass(frozen=True)
class IGDBConfig:
    min_interval_s: float = 0.3
    search_limit: int = 25
    get_by_ids_batch_size: int = 50


@dataclass(frozen=True)
class RAWGConfig:
    min_interval_s: float = 0.5


@dataclass(frozen=True)
class SteamSpyConfig:
    min_interval_s: float = 0.5


@dataclass(frozen=True)
class CLIConfig:
    igdb_flush_batch_size: int = 50
    steam_flush_batch_size: int = 25
    steam_streaming_flush_batch_size: int = 10


RETRY = RetryConfig()
MATCHING = MatchingConfig()
VALIDATION = ValidationConfig()
REQUEST = RequestConfig()
STEAM = SteamConfig()
IGDB = IGDBConfig()
RAWG = RAWGConfig()
STEAMSPY = SteamSpyConfig()
CLI = CLIConfig()
