{#
    Staging: procedures
    -------------------
    151,636 rows. NO primary key -- natural key is
    (patient, encounter, procedure_code, started_at).

    Columns are in SOURCE ORDER.

    START IS A RESERVED WORD IN SNOWFLAKE and must be quoted as "START".
    STOP is not reserved.

    Legitimately null: REASONCODE / REASONDESCRIPTION for 77,642 rows -- most
    procedures have no coded reason.

    CODE is a SNOMED/CPT code held as a STRING upstream. Never cast it: some
    values have leading zeros and SNOMED codes reach 17 digits.
#}

with source as (

    select * from {{ source('raw', 'procedures') }}

),

renamed as (

    select
        "START"                 as started_at,        -- reserved word, must quote
        STOP                    as stopped_at,
        PATIENT                 as patient_id,        -- foreign key -> patients
        ENCOUNTER               as encounter_id,      -- foreign key -> encounters
        SYSTEM                  as code_system,       -- e.g. SNOMED-CT
        CODE                    as procedure_code,    -- string, may have leading zeros
        DESCRIPTION             as description,
        BASE_COST               as base_cost,
        REASONCODE              as reason_code,
        REASONDESCRIPTION       as reason_description

    from source

)

select * from renamed
