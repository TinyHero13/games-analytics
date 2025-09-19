
with 
    dim_game_mode as (
        select distinct
            game_mode_id as game_mode_key
            , game_mode as game_mode_name
            , current_timestamp() as updated_at
        from {{ ref('stg__game_modes') }}
        where game_mode is not null
    )

select * 
from dim_game_mode