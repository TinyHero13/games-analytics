
with 
    game_platforms_bridge as (
        select 
            igdb_id
            , platform_id as platform_key
        from {{ ref('stg__platforms') }}
    )

select * 
from game_platforms_bridge