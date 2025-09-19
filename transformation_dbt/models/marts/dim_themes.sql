
with 
    dim_theme as (
        select distinct
            theme_id as theme_key
            , theme as theme_name
            , current_timestamp() as updated_at
        from {{ ref('stg__themes') }}
        where theme is not null
    )

select * 
from dim_theme