with 
    games_base as (
        select * from {{ ref('stg__igdb') }}
    )
    
    , companies as (
        select * from {{ ref('stg__companies') }}
    )
    
    , games_with_companies as (
        select 
            games_base.igdb_id
            , games_base.name
            , games_base.summary
            , games_base.total_rating
            , games_base.total_rating_count
            , games_base.release_date
            , companies.company_name
            , companies.is_developer
            , companies.is_publisher
        from games_base
        left join companies 
            on games_base.igdb_id = companies.igdb_id
    )

select * 
from games_with_companies