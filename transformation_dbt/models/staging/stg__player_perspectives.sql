with 
    player_perspectives as (
        select 
            cast(id as integer) as igdb_id
            , player_perspective.name as player_perspective
        from {{ source('games_sources', 'igdb_games') }}
        lateral view explode(from_json(player_perspectives, 'array<struct<id:int,name:string>>')) as player_perspective
    )

select *
from player_perspectives
