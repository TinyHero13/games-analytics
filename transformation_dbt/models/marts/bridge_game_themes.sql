
with 
    game_themes_bridge as (
        select 
            igdb_id
            , theme_id as theme_key
        from {{ ref('stg__themes') }}
    )

select * 
from game_themes_bridge