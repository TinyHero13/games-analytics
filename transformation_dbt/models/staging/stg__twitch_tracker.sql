with 
    twitch_tracker as (
        select 
            cast(id as integer) as twitch_tracker_id
            , cast(avg_viewers as integer) as average_viewers
            , cast(avg_channels as integer) as average_channels
            , cast(rank as integer) as rank
            , cast(hours_watched as integer) as hours_watched
            , ingestion_date
        from {{ source('games_sources', 'twitch_tracker') }}
    )

select *
from twitch_tracker