with 
    game_genres_bridge as (
        select 
            igdb_id
            , genre_id as genre_key
        from {{ ref('stg__genres') }}
    )

select * 
from game_genres_bridge