with 
    game_modes as (
        select 
            cast(id as integer) as igdb_id
            , game_mode.name as game_mode
            , game_mode.id as game_mode_id
        from {{ source('games_sources', 'igdb_games') }}
        lateral view explode(from_json(game_modes, 'array<struct<id:int,name:string>>')) as game_mode
    )

select *
from game_modes
