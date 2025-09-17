create or refresh materialized view stg_twitch (
    twitch_id integer 
    , name string
    , box_art_url string
    , igdb_id integer 
    , ingestion_timestamp timestamp
)
as
with
    source_data as (
        select *
        from (games_analytics.dev.twitch)
    )

select 
    cast(id as integer) as twitch_id
    , name
    , box_art_url
    , cast(igdb_id as integer) as igdb_id
    , ingestion_timestamp
from source_data;