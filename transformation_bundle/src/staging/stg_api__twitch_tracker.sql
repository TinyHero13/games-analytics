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
    , avg_viewers
    , avg_channels
    , rank
    , hours_watched
    , ingestion_timestamp
from source_data;