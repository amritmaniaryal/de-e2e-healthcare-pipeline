{#
    Staging: providers
    ------------------
    One row per provider, 152 rows.

    Columns are in SOURCE ORDER so this list can be diffed line-for-line
    against `select * from healthcare.raw.providers`.

    TWO TRAPS in this table:

    1. ENCOUNTERS and PROCEDURES are COUNTS of what that provider did -- they
       are integers, not foreign keys. Do not name them encounter_id /
       procedure_id, and do not join on them.

    2. The source spells it SPECIALITY (with an I). Renamed here to the correct
       `specialty`, which is what the whole project should use from now on.

    No reserved-word columns here, so nothing needs quoting.
#}

with source as (

    select * from {{ source('raw', 'providers') }}

),

renamed as (

    select
        Id                      as provider_id,        -- primary key
        ORGANIZATION            as organization_id,    -- foreign key -> organizations
        NAME                    as name,
        GENDER                  as gender,
        SPECIALITY              as specialty,          -- source spells this SPECIALITY
        ADDRESS                 as address,
        CITY                    as city,
        STATE                   as state,
        ZIP                     as zip,
        LAT                     as lat,
        LON                     as lon,
        ENCOUNTERS              as encounter_count,    -- a count, NOT a key
        PROCEDURES              as procedure_count,    -- a count, NOT a key
        NPI                     as npi                 -- 10-digit identifier, string

    from source

)

select * from renamed
