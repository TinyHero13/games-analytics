create or refresh materialized view stg_twitch_tracker (
    twitch_tracker_id integer 
    , average_viewers integer
    , average_channels integer
    , rank integer 
    , hours_watched integer
    , ingestion_timestamp timestamp
)
as
with
    source_data as (
        select *
        from (games_analytics.raw.twitch_tracker)
    )

select 
    cast(id as integer) as twitch_tracker_id
    , cast(avg_viewers as integer) as average_viewers
    , cast(avg_channels as integer) as average_channels
    , cast(rank as integer) as rank
    , cast(hours_watched as integer) as hours_watched
    , ingestion_timestamp
from source_data;