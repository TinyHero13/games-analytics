create or refresh materialized view game_genres (
    igdb_id integer 
    , genre string
)
as
with
    source_data as (
        select *
        from (games_analytics.raw.igdb_games)
    )

select 
    cast(id as integer) as igdb_id
    , genre.name as genre
from source_data
lateral view explode(from_json(genres, 'array<struct<id:int,name:string>>')) as genre;
