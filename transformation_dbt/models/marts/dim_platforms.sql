
with 
    dim_platform as (
        select distinct
            platform_id as platform_key
            , platform as platform_name
            , current_timestamp() as updated_at
        from {{ ref('stg__platforms') }}
        where platform is not null
    )

select * 
from dim_platform