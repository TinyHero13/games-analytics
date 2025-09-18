with 
    igdb as (
        select 
            cast(id as integer) as igdb_id
            , name
            , summary
            , cast(total_rating as double) as total_rating
            , cast(total_rating_count as integer) as total_rating_count
            , cast(from_unixtime(first_release_date) as date) as release_date
        from {{ source('games_sources', 'igdb_games') }}
    )

select *
from igdb