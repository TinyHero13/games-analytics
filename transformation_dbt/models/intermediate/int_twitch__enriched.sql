with 
    twitch as (
        select * from {{ ref('stg__twitch') }}
    )
    
    , twitch_tracker as (
        select * from {{ ref('stg__twitch_tracker') }}
    )
    
    , twitch_with_tracker as (
        select 
            twitch.twitch_id
            , twitch.game_name
            , twitch.box_art_url
            , twitch.igdb_id
            , twitch_tracker.average_viewers
            , twitch_tracker.average_channels
            , twitch_tracker.rank as twitch_rank
            , twitch_tracker.hours_watched
        from twitch
        left join twitch_tracker
            on twitch.twitch_id = twitch_tracker.twitch_tracker_id
    )

select * 
from twitch_with_tracker