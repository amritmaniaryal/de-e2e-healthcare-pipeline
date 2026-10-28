{#
    Staging: medications
    --------------------
    39,729 rows in the warehouse (39,802 in the source CSV -- 73 were genuine
    duplicates removed by the Spark clean step). NO primary key -- natural key
    is (patient, encounter, medication_code, started_at).

    Columns are in SOURCE ORDER.

    START IS A RESERVED WORD IN SNOWFLAKE and must be quoted as "START".
    STOP is not reserved.

    Legitimately null: STOP 2,540, REASONCODE / REASONDESCRIPTION 8,978.

    DISPENSES is a COUNT, not a key. BASE_COST, PAYER_COVERAGE, TOTALCOST are
    decimal(12,2).
#}

with source as (

    select * from {{ source('raw', 'medications') }}

),

renamed as (

    select
        "START"                 as started_at,        -- reserved word, must quote
        STOP                    as stopped_at,
        PATIENT                 as patient_id,        -- foreign key -> patients
        PAYER                   as payer_id,          -- foreign key -> payers
        ENCOUNTER               as encounter_id,      -- foreign key -> encounters
        CODE                    as medication_code,   -- RxNorm, string
        DESCRIPTION             as description,
        BASE_COST               as base_cost,
        PAYER_COVERAGE          as payer_coverage,
        DISPENSES               as dispenses,         -- a count, NOT a key
        TOTALCOST               as total_cost,
        REASONCODE              as reason_code,
        REASONDESCRIPTION       as reason_description

    from source

)

select * from renamed
