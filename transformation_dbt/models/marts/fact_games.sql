with 
    games_base as (
        select * 
        from {{ ref('stg__igdb') }}
    )
    
    , twitch_performance as (
        select 
            igdb_id
            , average_viewers
            , average_channels
            , twitch_rank
            , hours_watched
        from {{ ref('int_twitch__enriched') }}
        where igdb_id is not null
    )
    
    , bridge_companies as (
        select 
            igdb_id
            , count(*) as total_companies
            , count(case when is_developer then 1 end) as total_developers
            , count(case when is_publisher then 1 end) as total_publishers
            , min(case when is_developer then company_key end) as primary_developer_key
            , min(case when is_publisher then company_key end) as primary_publisher_key
        from {{ ref('bridge_game_companies') }}
        group by igdb_id
    )
    
    , bridge_genres as (
        select 
            igdb_id
            , count(*) as total_genres
            , array_agg(genre_key) as genre_keys
        from {{ ref('bridge_game_genres') }}
        group by igdb_id
    )
    
    , bridge_keywords as (
        select 
            igdb_id
            , count(*) as total_keywords
        from {{ ref('bridge_game_keywords') }}
        group by igdb_id
    )
    
    , bridge_languages as (
        select 
            igdb_id
            , count(*) as total_languages
            , count(case when language_support_type_desc = 'audio' then 1 end) as audio_languages
            , count(case 
                when language_support_type_desc = 'subtitles' 
                then 1 
            end) as subtitle_languages
            , count(case 
                when language_support_type_desc = 'interface' 
                then 1 
            end) as interface_languages
        from {{ ref('bridge_game_language_support') }}
        group by igdb_id
    )
    
    , bridge_modes as (
        select 
            bridge_game_modes.igdb_id
            , count(*) as total_game_modes
            , max(case 
                when dim_game_modes.game_mode_name = 'Single player' 
                then 1 
                else 0 
            end) as has_single_player
            , max(case 
                when dim_game_modes.game_mode_name = 'Multiplayer' 
                then 1 
                else 0 
            end) as has_multiplayer
            , max(case 
                when dim_game_modes.game_mode_name = 'Co-operative' 
                then 1 
                else 0 
            end) as has_coop
            , array_agg(bridge_game_modes.game_mode_key) as game_mode_keys
        from {{ ref('bridge_game_modes') }} bridge_game_modes
        left join {{ ref('dim_game_modes') }} dim_game_modes 
            on bridge_game_modes.game_mode_key = dim_game_modes.game_mode_key
        group by bridge_game_modes.igdb_id
    )
    
    , bridge_platforms as (
        select 
            igdb_id
            , count(*) as total_platforms
            , array_agg(platform_key) as platform_keys
        from {{ ref('bridge_game_platforms') }}
        group by igdb_id
    )
    
    , bridge_themes as (
        select 
            igdb_id
            , count(*) as total_themes
        from {{ ref('bridge_game_themes') }}
        group by igdb_id
    )
    
    , fact_games as (
        select 
            games_base.igdb_id
            , games_base.name as game_name
            , games_base.summary
            , games_base.total_rating
            , games_base.total_rating_count
            , games_base.release_date
            , coalesce(bridge_companies.total_companies, 0) as total_companies
            , coalesce(bridge_companies.total_developers, 0) as total_developers
            , coalesce(bridge_companies.total_publishers, 0) as total_publishers
            , bridge_companies.primary_developer_key
            , bridge_companies.primary_publisher_key
            , coalesce(bridge_genres.total_genres, 0) as total_genres
            , bridge_genres.genre_keys
            , coalesce(bridge_keywords.total_keywords, 0) as total_keywords
            , coalesce(bridge_languages.total_languages, 0) as total_languages
            , coalesce(bridge_languages.audio_languages, 0) as audio_languages
            , coalesce(bridge_languages.subtitle_languages, 0) as subtitle_languages
            , coalesce(bridge_languages.interface_languages, 0) as interface_languages
            , coalesce(bridge_modes.total_game_modes, 0) as total_game_modes
            , coalesce(bridge_modes.has_single_player, 0) as has_single_player
            , coalesce(bridge_modes.has_multiplayer, 0) as has_multiplayer
            , coalesce(bridge_modes.has_coop, 0) as has_coop
            , bridge_modes.game_mode_keys
            , coalesce(bridge_platforms.total_platforms, 0) as total_platforms
            , bridge_platforms.platform_keys
            , coalesce(bridge_themes.total_themes, 0) as total_themes
            , coalesce(twitch_performance.average_viewers, 0) as twitch_viewers
            , coalesce(twitch_performance.average_channels, 0) as twitch_channels
            , coalesce(twitch_performance.twitch_rank, 999999) as twitch_rank
            , coalesce(twitch_performance.hours_watched, 0) as hours_watched
            , current_timestamp() as updated_at
        from games_base
        left join twitch_performance on games_base.igdb_id = twitch_performance.igdb_id
        left join bridge_companies on games_base.igdb_id = bridge_companies.igdb_id
        left join bridge_genres on games_base.igdb_id = bridge_genres.igdb_id
        left join bridge_keywords on games_base.igdb_id = bridge_keywords.igdb_id
        left join bridge_languages on games_base.igdb_id = bridge_languages.igdb_id
        left join bridge_modes on games_base.igdb_id = bridge_modes.igdb_id
        left join bridge_platforms on games_base.igdb_id = bridge_platforms.igdb_id
        left join bridge_themes on games_base.igdb_id = bridge_themes.igdb_id
    )

select * 
from fact_games