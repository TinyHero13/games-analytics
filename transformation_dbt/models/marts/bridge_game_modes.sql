with 
    game_modes_bridge as (
        select 
            igdb_id
            , game_mode_id as game_mode_key
        from {{ ref('stg__game_modes') }}
    )

select * 
from game_modes_bridge