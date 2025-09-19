
with 
    dim_game as (
        select 
            igdb_id
            , name as game_name
            , summary
            , total_rating
            , total_rating_count
            , release_date
            , current_timestamp() as updated_at
        from {{ ref('stg__igdb') }}
    )

select * 
from dim_game