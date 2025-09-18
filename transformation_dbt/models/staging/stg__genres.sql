with 
    genres as (
        select 
            cast(id as integer) as igdb_id
            , genre.name as genre
            , genre.id as genre_id
        from {{ source('games_sources', 'igdb_games') }}
        lateral view explode(from_json(genres, 'array<struct<id:int,name:string>>')) as genre
    )

select *
from genres
