with 
    game_keywords_bridge as (
        select 
            igdb_id
            , keyword_id as keyword_key
        from {{ ref('stg__keywords') }}
    )

select * 
from game_keywords_bridge