{#
    Staging: conditions
    -------------------
    34,727 rows. NO primary key -- natural key is
    (patient, encounter, condition_code, started_at).

    Columns are in SOURCE ORDER.

    START IS A RESERVED WORD IN SNOWFLAKE and must be quoted as "START". The
    loaded column is uppercase, so "START" matches it exactly. STOP is not
    reserved and needs no quotes.

    Legitimately null: STOP (8,857 rows -- ongoing conditions).

    CODE is a SNOMED code (up to 17 digits) and is a STRING in the warehouse.
    Do not cast it to a number -- leading digits matter and 17 digits overflows.
#}

with source as (

    select * from {{ source('raw', 'conditions') }}

),

renamed as (

    select
        "START"                 as started_at,        -- reserved word, must quote
        STOP                    as stopped_at,        -- null if still active
        PATIENT                 as patient_id,        -- foreign key -> patients
        ENCOUNTER               as encounter_id,      -- foreign key -> encounters
        SYSTEM                  as code_system,       -- e.g. SNOMED-CT
        CODE                    as condition_code,    -- SNOMED, string
        DESCRIPTION             as description

    from source

)

select * from renamed
