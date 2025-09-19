
with 
    dim_genre as (
        select distinct
            genre_id as genre_key
            , genre as genre_name
            , current_timestamp() as updated_at
        from {{ ref('stg__genres') }}
        where genre is not null
    )

select * 
from dim_genre