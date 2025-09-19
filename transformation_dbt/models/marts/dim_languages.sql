
with 
    dim_language as (
        select 
            language_id
            , language_name
            , current_timestamp() as updated_at
        from {{ ref('int_languages__distinct') }}
    )

select * 
from dim_language