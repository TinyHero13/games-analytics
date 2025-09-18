with 
    companies as (
        select 
            cast(id as integer) as igdb_id
            , company.company.id as company_id
            , company.company.name as company_name
            , cast(company.developer as boolean) as is_developer
            , cast(company.publisher as boolean) as is_publisher
        from {{ source('games_sources', 'igdb_games') }}
        lateral view explode(
            from_json(involved_companies, 'array<struct<id:int,company:struct<id:int,name:string>,developer:boolean,publisher:boolean>>')
        ) as company
    )

select *
from companies