{#
    Staging: payer_transitions
    --------------------------
    37,700 rows. NO primary key -- this is an event log, keyed naturally by
    (patient, member_id, start_date, payer).

    Columns are in SOURCE ORDER.

    IMPORTANT: MEMBERID is null for 10,521 rows -- every government-payer row
    (Medicare, Medicaid have no member id). That is why a primary key including
    MEMBERID was wrong in the Spark layer and silently discarded 28% of the
    table. Do not treat those nulls as a defect.

    SECONDARY_PAYER is null for 34,697 rows -- most people have one payer.
#}

with source as (

    select * from {{ source('raw', 'payer_transitions') }}

),

renamed as (

    select
        PATIENT                 as patient_id,          -- foreign key -> patients
        MEMBERID                as member_id,           -- null for govt payers
        START_DATE              as start_date,
        END_DATE                as end_date,
        PAYER                   as payer_id,            -- foreign key -> payers
        SECONDARY_PAYER         as secondary_payer_id,  -- foreign key -> payers
        PLAN_OWNERSHIP          as plan_ownership,
        OWNER_NAME              as owner_name

    from source

)

select * from renamed
