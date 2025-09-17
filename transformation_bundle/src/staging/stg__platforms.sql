create or refresh materialized view stg_platforms (
    igdb_id integer 
    , platform string
)
as
with
    source_data as (
        select *
        from (games_analytics.raw.igdb_games)
    )

select 
    cast(id as integer) as igdb_id
    , platform.name as platform
from source_data
lateral view explode(from_json(platforms, 'array<struct<id:int,abbreviation:string,name:string>>')) as platform;