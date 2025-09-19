with 
    languages_distinct as (
        select distinct
            language_id,
            language_name,
            count(*) as total_games
        from {{ ref('stg__languages') }}
        where language_id is not null 
        and language_name is not null
        group by language_id, language_name
    )

select * 
from languages_distinct