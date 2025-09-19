with 
    themes as (
        select 
            cast(id as integer) as igdb_id
            , theme.name as theme
            , theme.id as theme_id
        from {{ source('games_sources', 'igdb_games') }}
        lateral view explode(from_json(themes, 'array<struct<id:int,name:string>>')) as theme
    )

select *
from themes
