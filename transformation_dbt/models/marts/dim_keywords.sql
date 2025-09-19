
with 
    dim_keyword as (
        select distinct
            keyword_id as keyword_key
            , keyword_name
            , current_timestamp() as updated_at
        from {{ ref('stg__keywords') }}
        where keyword_name is not null
    )

select * 
from dim_keyword