with 
    platforms as (
        select 
            cast(id as integer) as igdb_id
            , platform.name as platform
            , platform.id as platform_id
        from {{ source('games_sources', 'igdb_games') }}
        lateral view explode(from_json(platforms, 'array<struct<id:int,abbreviation:string,name:string>>')) as platform
    )

select *
from platforms