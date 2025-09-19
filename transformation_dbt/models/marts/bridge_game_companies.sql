with 
    game_companies_bridge as (
        select 
            igdb_id
            , company_id as company_key
            , is_developer
            , is_publisher
        from {{ ref('stg__companies') }}
    )

select * 
from game_companies_bridge