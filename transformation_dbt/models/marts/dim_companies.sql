
with 
    dim_company as (
        select 
            company_id as company_key
            , company_name
            , max(case when is_developer = true then true else false end) as is_developer
            , max(case when is_publisher = true then true else false end) as is_publisher
            , current_timestamp() as updated_at
        from {{ ref('stg__companies') }}
        where company_name is not null
        group by company_id, company_name
    )

select * 
from dim_company