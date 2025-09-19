with 
    languages as (
        select 
            cast(id as integer) as igdb_id
            , cast(languages.language.id as int) as language_id
            , languages.language.name as language_name
            , cast(languages.language_support_type as int) as language_support_type
        from {{ source('games_sources', 'igdb_games') }}
        lateral view explode(
            from_json(language_supports, 'array<struct<id:int,language:struct<id:int,name:string>,language_support_type:int>>')
        ) as languages
    )

select *
from languages