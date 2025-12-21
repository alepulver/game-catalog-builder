"""API clients for game data sources."""

from .hltb_client import HLTBClient
from .igdb_client import IGDBClient
from .rawg_client import RAWGClient
from .steam_client import SteamClient
from .steamspy_client import SteamSpyClient
from .wikidata_client import WikidataClient
from .wikipedia_pageviews_client import WikipediaPageviewsClient
from .wikipedia_summary_client import WikipediaSummaryClient

__all__ = [
    "HLTBClient",
    "IGDBClient",
    "RAWGClient",
    "SteamClient",
    "SteamSpyClient",
    "WikidataClient",
    "WikipediaPageviewsClient",
    "WikipediaSummaryClient",
]
