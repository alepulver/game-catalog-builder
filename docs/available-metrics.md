# Available Metrics

Canonical metric keys come from `data/metrics-registry.example.yaml` (version 2).
CSV columns are a view; internal JSONL uses these dotted keys.

## rawg

| Metric Key | CSV Column | Type |
|---|---|---|
| `rawg.id` | `RAWG_ID` | `string` |
| `rawg.name` | `RAWG_Name` | `string` |
| `rawg.name_original` | `RAWG_NameOriginal` | `string` |
| `rawg.released` | `RAWG_Released` | `string` |
| `rawg.year` | `RAWG_Year` | `int` |
| `rawg.website` | `RAWG_Website` | `string` |
| `rawg.description_raw` | `RAWG_DescriptionRaw` | `string` |
| `rawg.reddit_url` | `RAWG_RedditURL` | `string` |
| `rawg.metacritic_url` | `RAWG_MetacriticURL` | `string` |
| `rawg.background_image` | `RAWG_BackgroundImage` | `string` |
| `rawg.genres` | `RAWG_Genres` | `json` |
| `rawg.platforms` | `RAWG_Platforms` | `json` |
| `rawg.tags` | `RAWG_Tags` | `json` |
| `rawg.esrb` | `RAWG_ESRB` | `string` |
| `rawg.score_100` | `RAWG_Score_100` | `int` |
| `rawg.ratings_count` | `RAWG_RatingsCount` | `int` |
| `rawg.metacritic_100` | `RAWG_Metacritic` | `int` |
| `rawg.popularity.added_total` | `RAWG_Added` | `int` |
| `rawg.popularity.added_by_status.owned` | `RAWG_AddedByStatusOwned` | `int` |
| `rawg.popularity.added_by_status.playing` | `RAWG_AddedByStatusPlaying` | `int` |
| `rawg.popularity.added_by_status.beaten` | `RAWG_AddedByStatusBeaten` | `int` |
| `rawg.popularity.added_by_status.toplay` | `RAWG_AddedByStatusToplay` | `int` |
| `rawg.popularity.added_by_status.dropped` | `RAWG_AddedByStatusDropped` | `int` |
| `rawg.developers` | `RAWG_Developers` | `json` |
| `rawg.publishers` | `RAWG_Publishers` | `json` |

## igdb

| Metric Key | CSV Column | Type |
|---|---|---|
| `igdb.id` | `IGDB_ID` | `string` |
| `igdb.name` | `IGDB_Name` | `string` |
| `igdb.year` | `IGDB_Year` | `int` |
| `igdb.summary` | `IGDB_Summary` | `string` |
| `igdb.websites` | `IGDB_Websites` | `json` |
| `igdb.alternative_names` | `IGDB_AlternativeNames` | `json` |
| `igdb.platforms` | `IGDB_Platforms` | `json` |
| `igdb.genres` | `IGDB_Genres` | `json` |
| `igdb.themes` | `IGDB_Themes` | `json` |
| `igdb.keywords` | `IGDB_Keywords` | `json` |
| `igdb.game_modes` | `IGDB_GameModes` | `json` |
| `igdb.perspectives` | `IGDB_Perspectives` | `json` |
| `igdb.franchise` | `IGDB_Franchise` | `json` |
| `igdb.engine` | `IGDB_Engine` | `json` |
| `igdb.relationships.parent_game` | `IGDB_ParentGame` | `string` |
| `igdb.relationships.version_parent` | `IGDB_VersionParent` | `string` |
| `igdb.relationships.dlcs` | `IGDB_DLCs` | `json` |
| `igdb.relationships.expansions` | `IGDB_Expansions` | `json` |
| `igdb.relationships.ports` | `IGDB_Ports` | `json` |
| `igdb.cross_ids.steam_app_id` | `IGDB_SteamAppID` | `string` |
| `igdb.developers` | `IGDB_Developers` | `json` |
| `igdb.publishers` | `IGDB_Publishers` | `json` |
| `igdb.score_count` | `IGDB_ScoreCount` | `int` |
| `igdb.score_100` | `IGDB_Score_100` | `int` |
| `igdb.critic_score_count` | `IGDB_CriticScoreCount` | `int` |
| `igdb.critic.score_100` | `IGDB_CriticScore_100` | `int` |

## steam

| Metric Key | CSV Column | Type |
|---|---|---|
| `steam.app_id` | `Steam_AppID` | `string` |
| `steam.name` | `Steam_Name` | `string` |
| `steam.url` | `Steam_URL` | `string` |
| `steam.website` | `Steam_Website` | `string` |
| `steam.short_description` | `Steam_ShortDescription` | `string` |
| `steam.store_type` | `Steam_StoreType` | `string` |
| `steam.release_year` | `Steam_ReleaseYear` | `int` |
| `steam.platforms` | `Steam_Platforms` | `json` |
| `steam.tags` | `Steam_Tags` | `json` |
| `steam.review_count` | `Steam_ReviewCount` | `int` |
| `steam.price` | `Steam_Price` | `string` |
| `steam.categories` | `Steam_Categories` | `json` |
| `steam.metacritic_100` | `Steam_Metacritic` | `int` |
| `steam.developers` | `Steam_Developers` | `json` |
| `steam.publishers` | `Steam_Publishers` | `json` |

## steamspy

| Metric Key | CSV Column | Type |
|---|---|---|
| `steamspy.owners` | `SteamSpy_Owners` | `string` |
| `steamspy.players` | `SteamSpy_Players` | `int` |
| `steamspy.players_2weeks` | `SteamSpy_Players2Weeks` | `int` |
| `steamspy.ccu` | `SteamSpy_CCU` | `int` |
| `steamspy.playtime_avg` | `SteamSpy_PlaytimeAvg` | `int` |
| `steamspy.playtime_avg_2weeks` | `SteamSpy_PlaytimeAvg2Weeks` | `int` |
| `steamspy.playtime_median_2weeks` | `SteamSpy_PlaytimeMedian2Weeks` | `int` |
| `steamspy.playtime_median` | `SteamSpy_PlaytimeMedian` | `int` |
| `steamspy.positive` | `SteamSpy_Positive` | `int` |
| `steamspy.negative` | `SteamSpy_Negative` | `int` |
| `steamspy.price` | `SteamSpy_Price` | `int` |
| `steamspy.initial_price` | `SteamSpy_InitialPrice` | `int` |
| `steamspy.discount_percent` | `SteamSpy_DiscountPercent` | `int` |
| `steamspy.developer` | `SteamSpy_Developer` | `string` |
| `steamspy.publisher` | `SteamSpy_Publisher` | `string` |
| `steamspy.popularity.tags` | `SteamSpy_Tags` | `json` |
| `steamspy.popularity.tags_top` | `SteamSpy_TagsTop` | `json` |
| `steamspy.score_100` | `SteamSpy_Score_100` | `int` |

## hltb

| Metric Key | CSV Column | Type |
|---|---|---|
| `hltb.name` | `HLTB_Name` | `string` |
| `hltb.time.main` | `HLTB_Main` | `float` |
| `hltb.time.extra` | `HLTB_Extra` | `float` |
| `hltb.time.completionist` | `HLTB_Completionist` | `float` |
| `hltb.release_year` | `HLTB_ReleaseYear` | `int` |
| `hltb.platforms` | `HLTB_Platforms` | `json` |
| `hltb.score_100` | `HLTB_Score_100` | `int` |
| `hltb.url` | `HLTB_URL` | `string` |
| `hltb.aliases` | `HLTB_Aliases` | `json` |

## wikidata

| Metric Key | CSV Column | Type |
|---|---|---|
| `wikidata.qid` | `Wikidata_QID` | `string` |
| `wikidata.label` | `Wikidata_Label` | `string` |
| `wikidata.description` | `Wikidata_Description` | `string` |
| `wikidata.release_year` | `Wikidata_ReleaseYear` | `int` |
| `wikidata.release_date` | `Wikidata_ReleaseDate` | `string` |
| `wikidata.developers` | `Wikidata_Developers` | `json` |
| `wikidata.publishers` | `Wikidata_Publishers` | `json` |
| `wikidata.platforms` | `Wikidata_Platforms` | `json` |
| `wikidata.series` | `Wikidata_Series` | `json` |
| `wikidata.genres` | `Wikidata_Genres` | `json` |
| `wikidata.instance_of` | `Wikidata_InstanceOf` | `json` |
| `wikidata.enwiki_title` | `Wikidata_EnwikiTitle` | `string` |
| `wikidata.wikipedia` | `Wikidata_Wikipedia` | `string` |

## derived

| Metric Key | CSV Column | Type |
|---|---|---|
| `derived.reach.steamspy_owners_low` | `Reach_SteamSpyOwners_Low` | `int` |
| `derived.reach.steamspy_owners_high` | `Reach_SteamSpyOwners_High` | `int` |
| `derived.reach.steamspy_owners_mid` | `Reach_SteamSpyOwners_Mid` | `int` |
| `derived.reach.steam_reviews` | `Reach_SteamReviews` | `int` |
| `derived.reach.rawg_ratings_count` | `Reach_RAWGRatingsCount` | `int` |
| `derived.reach.igdb_rating_count` | `Reach_IGDBRatingCount` | `int` |
| `derived.reach.igdb_aggregated_rating_count` | `Reach_IGDBAggregatedRatingCount` | `int` |
| `derived.companies.developers_consensus_providers` | `Developers_ConsensusProviders` | `string` |
| `derived.companies.developers_consensus` | `Developers_Consensus` | `list_csv` |
| `derived.companies.developers_consensus_provider_count` | `Developers_ConsensusProviderCount` | `int` |
| `derived.companies.publishers_consensus_providers` | `Publishers_ConsensusProviders` | `string` |
| `derived.companies.publishers_consensus` | `Publishers_Consensus` | `list_csv` |
| `derived.companies.publishers_consensus_provider_count` | `Publishers_ConsensusProviderCount` | `int` |
| `derived.content_type.value` | `ContentType` | `string` |
| `derived.content_type.consensus_providers` | `ContentType_ConsensusProviders` | `string` |
| `derived.content_type.source_signals` | `ContentType_SourceSignals` | `string` |
| `derived.content_type.conflict` | `ContentType_Conflict` | `string` |
| `derived.igdb.has_dlcs` | `HasDLCs` | `bool` |
| `derived.igdb.has_expansions` | `HasExpansions` | `bool` |
| `derived.igdb.has_ports` | `HasPorts` | `bool` |
| `derived.genre.main` | `Genre_Main` | `string` |
| `derived.genre.sources` | `Genre_MainSources` | `string` |
| `derived.replayability.score_100` | `Replayability_100` | `int` |
| `derived.replayability.source_signals` | `Replayability_SourceSignals` | `string` |
| `derived.modding.has_workshop` | `HasWorkshop` | `bool` |
| `derived.modding.score_100` | `ModdingSignal_100` | `int` |
| `derived.modding.source_signals` | `Modding_SourceSignals` | `string` |
| `derived.production.tier` | `Production_Tier` | `string` |
| `derived.production.tier_reason` | `Production_TierReason` | `string` |
| `derived.now.steamspy_playtime_avg_2weeks` | `Now_SteamSpyPlaytimeAvg2Weeks` | `int` |
| `derived.now.steamspy_playtime_median_2weeks` | `Now_SteamSpyPlaytimeMedian2Weeks` | `int` |

## composite

| Metric Key | CSV Column | Type |
|---|---|---|
| `composite.reach.score_100` | `Reach_Composite` | `int` |
| `composite.launch_interest.score_100` | `Launch_Interest_100` | `int` |
| `composite.community_rating.score_100` | `CommunityRating_Composite_100` | `int` |
| `composite.critic_rating.score_100` | `CriticRating_Composite_100` | `int` |
| `composite.now.score_100` | `Now_Composite` | `int` |

## other

| Metric Key | CSV Column | Type |
|---|---|---|
| `wikipedia.page_url` | `Wikidata_WikipediaPage` | `string` |
| `wikipedia.summary` | `Wikidata_WikipediaSummary` | `string` |
| `wikipedia.thumbnail` | `Wikidata_WikipediaThumbnail` | `string` |
| `wikipedia.pageviews_30d` | `Wikidata_Pageviews30d` | `int` |
| `wikipedia.pageviews_90d` | `Wikidata_Pageviews90d` | `int` |
| `wikipedia.pageviews_365d` | `Wikidata_Pageviews365d` | `int` |
| `wikipedia.pageviews_first_30d` | `Wikidata_PageviewsFirst30d` | `int` |
| `wikipedia.pageviews_first_90d` | `Wikidata_PageviewsFirst90d` | `int` |
