with 
    similar_games as (
        select 
            cast(id as integer) as igdb_id
            , similar_game_data.name as similar_game_name
        from {{ source('games_sources', 'igdb_games') }}
        lateral view explode(
            from_json(similar_games, 'array<struct<id:int,name:string>>')
        ) as similar_game_data
    )

select *
from similar_games