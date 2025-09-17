create or refresh materialized view stg_igdb (
    igdb_id integer 
    , name string
    , summary string
    , total_rating double
    , total_rating_count integer 
    , release_date date
    , ingestion_timestamp timestamp
)
as
with
    source_data as (
        select *
        from (games_analytics.raw.igdb_games)
    )

select 
    cast(id as integer) as igdb_id
    , name
    , summary
    , cast(total_rating as double) as total_rating
    , cast(total_rating_count as integer) as total_rating_count
    , cast(from_unixtime(first_release_date) as date) as release_date
    , ingestion_timestamp
from source_data;