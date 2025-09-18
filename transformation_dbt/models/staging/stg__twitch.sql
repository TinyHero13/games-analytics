with 
    twitch as (
        select 
            cast(id as int) as twitch_id
            , name as game_name
            , box_art_url
            , try_cast(igdb_id as int) as igdb_id
            , ingestion_date
        from {{ source('games_sources', 'twitch') }}
    )

select *
from twitch