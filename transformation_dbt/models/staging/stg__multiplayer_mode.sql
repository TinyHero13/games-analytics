with 
    multiplayer_modes as (
        select 
            cast(id as integer) as igdb_id
            , cast(multiplayer_mode.campaigncoop as boolean) as campaign_coop
            , cast(multiplayer_mode.dropin as boolean) as drop_in
            , cast(multiplayer_mode.lancoop as boolean) as lan_coop
            , cast(multiplayer_mode.offlinecoop as boolean) as offline_coop
            , cast(multiplayer_mode.offlinecoopmax as integer) as offline_coop_max
            , cast(multiplayer_mode.offlinemax as integer) as offline_max
            , cast(multiplayer_mode.onlinecoop as boolean) as online_coop
            , cast(multiplayer_mode.onlinecoopmax as integer) as online_coop_max
            , cast(multiplayer_mode.onlinemax as integer) as online_max
            , cast(multiplayer_mode.platform as integer) as platform_id
            , cast(multiplayer_mode.splitscreen as boolean) as split_screen
        from {{ source('games_sources', 'igdb_games') }}
        lateral view explode(
            from_json(multiplayer_modes, 'array<struct<id:int,campaigncoop:boolean,dropin:boolean,lancoop:boolean,offlinecoop:boolean,offlinecoopmax:int,offlinemax:int,onlinecoop:boolean,onlinecoopmax:int,onlinemax:int,platform:int,splitscreen:boolean>>')
        ) as multiplayer_mode
    )

select *
from multiplayer_modes