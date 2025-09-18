with 
    keywords as (
        select 
            cast(id as integer) as igdb_id
            , cast(keyword_data.id as integer) as keyword_id
            , keyword_data.name as keyword_name
        from {{ source('games_sources', 'igdb_games') }}
        lateral view explode(
            from_json(keywords, 'array<struct<id:int,name:string>>')
        ) as keyword_data
    )

select *
from keywords