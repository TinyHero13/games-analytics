with 
    language_support_bridge as (
        select 
            igdb_id
            , language_id
            , language_support_type
            , case 
                when language_support_type = 1 
                    then 'audio'
                when language_support_type = 2 
                    then 'subtitles'
                when language_support_type = 3 
                    then 'interface'
                else 'unknown'
            end as language_support_type_desc
        from {{ ref('stg__languages') }}
        where igdb_id is not null 
        and language_id is not null 
        and language_support_type is not null
    )

select * 
from language_support_bridge