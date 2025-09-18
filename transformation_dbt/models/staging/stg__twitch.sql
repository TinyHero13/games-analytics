with 
    twitch as (
        select 
            cast(id as int) as twitch_id
            , name as game_name
            , box_art_url
            , cast(igdb_id as int) as igdb_id
            , ingestion_timestamp
        from {{ source('games_sources', 'twitch') }}
    )

select *
from twitch