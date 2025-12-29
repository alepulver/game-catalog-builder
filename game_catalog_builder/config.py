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
    year_hint_tolerance: int = 2


@dataclass(frozen=True)
class ValidationConfig:
    title_score_warn: int = 90
    year_max_diff: int = 1


@dataclass(frozen=True)
class RequestConfig:
    timeout_s: int = 10


@dataclass(frozen=True)
class CacheConfig:
    # Minimum time between JSON cache rewrites per provider/client (throttled writes).
    # A final flush is attempted at process exit.
    save_min_interval_small_s: float = 10.0
    save_min_interval_large_s: float = 60.0
    # For very large caches (hundreds of MB), writes can still dominate runtime; throttle a bit.
    save_min_interval_huge_s: float = 60.0
    # Log cache writes that take longer than this threshold (milliseconds).
    slow_save_log_ms: int = 2000


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
class HLTBConfig:
    # Stop trying additional query variants once we have a high-confidence match.
    early_stop_score: int = 95


@dataclass(frozen=True)
class WikidataConfig:
    min_interval_s: float = 0.25
    search_limit: int = 20
    labels_batch_size: int = 50
    get_by_ids_batch_size: int = 50


@dataclass(frozen=True)
class CLIConfig:
    igdb_flush_batch_size: int = 50
    steam_flush_batch_size: int = 25
    steam_streaming_flush_batch_size: int = 10
    progress_every_n: int = 25
    progress_min_interval_s: float = 30.0
    max_parallel_providers: int = 8


@dataclass(frozen=True)
class SignalsConfig:
    """
    Scaling constants for derived "composite" signals.

    These are intentionally simple and monotonic (log-scaled counts -> 0..100) so that:
    - values remain comparable across rows,
    - we avoid fragile dataset-percentile normalization.
    """

    # log10 scale bounds for different reach proxies
    reach_owners_log10_min: float = 3.0  # 1k
    reach_owners_log10_max: float = 8.0  # 100M
    reach_reviews_log10_min: float = 2.0  # 100
    reach_reviews_log10_max: float = 7.0  # 10M
    reach_votes_log10_min: float = 2.0  # 100
    reach_votes_log10_max: float = 6.0  # 1M
    reach_rawg_added_log10_min: float = 2.0  # 100
    reach_rawg_added_log10_max: float = 7.0  # 10M
    reach_critic_votes_log10_min: float = 1.0  # 10
    reach_critic_votes_log10_max: float = 5.0  # 100k
    reach_pageviews_log10_min: float = 2.0  # 100
    reach_pageviews_log10_max: float = 8.0  # 100M

    # "Now" (recent activity) proxies
    now_players2w_log10_min: float = 1.0  # 10
    now_players2w_log10_max: float = 7.0  # 10M
    now_ccu_log10_min: float = 0.0  # 1
    now_ccu_log10_max: float = 6.0  # 1M
    now_pageviews_log10_min: float = 1.0  # 10
    now_pageviews_log10_max: float = 7.0  # 10M

    # weights used for the composite blend
    w_owners: float = 3.0
    w_reviews: float = 2.0
    w_votes: float = 1.0
    w_rawg_added: float = 1.0
    w_critic_votes: float = 0.5
    w_pageviews: float = 0.8
    w_players2w: float = 2.0
    w_ccu: float = 1.0
    w_now_pageviews: float = 1.0


RETRY = RetryConfig()
MATCHING = MatchingConfig()
VALIDATION = ValidationConfig()
REQUEST = RequestConfig()
CACHE = CacheConfig()
STEAM = SteamConfig()
IGDB = IGDBConfig()
RAWG = RAWGConfig()
STEAMSPY = SteamSpyConfig()
HLTB = HLTBConfig()
WIKIDATA = WikidataConfig()
CLI = CLIConfig()
SIGNALS = SignalsConfig()
