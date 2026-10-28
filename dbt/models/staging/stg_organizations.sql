{#
    Staging: organizations
    ----------------------
    One row per organisation, 152 rows. No foreign keys.

    Columns are in SOURCE ORDER so this list can be diffed line-for-line
    against `select * from healthcare.raw.organizations`.

    NOTE: UTILIZATION is a COUNT of encounters (integers like 1096, 70), not a
    ratio and not a key. Same trap as providers.ENCOUNTERS.
#}

with source as (

    select * from {{ source('raw', 'organizations') }}

),

renamed as (

    select
        Id                      as organization_id,    -- primary key
        NAME                    as name,
        ADDRESS                 as address,
        CITY                    as city,
        STATE                   as state,
        ZIP                     as zip,
        LAT                     as lat,
        LON                     as lon,
        PHONE                   as phone,
        REVENUE                 as revenue,
        UTILIZATION             as utilization,        -- a count, NOT a key
        NPI                     as npi                 -- 10-digit identifier, string

    from source

)

select * from renamed
