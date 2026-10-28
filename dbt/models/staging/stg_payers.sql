{#
    Staging: payers
    ---------------
    One row per payer, 10 rows. No foreign keys.

    Columns are in SOURCE ORDER.

    IMPORTANT NULLS: ADDRESS, CITY, STATE_HEADQUARTERED, ZIP and PHONE are
    blank in ALL 10 rows -- Synthea does not populate them for payers. Five
    permanently empty columns; do not add not_null tests to them.

    The COVERED_* / UNCOVERED_* columns are COUNTS of services, not keys.
    AMOUNT_*, REVENUE are decimal(12,2). QOLS_AVG is a quality-of-life score
    between 0 and 1, so double not decimal.
#}

with source as (

    select * from {{ source('raw', 'payers') }}

),

renamed as (

    select
        Id                          as payer_id,        -- primary key
        NAME                        as name,
        OWNERSHIP                   as ownership,
        ADDRESS                     as address,         -- always null
        CITY                        as city,            -- always null
        STATE_HEADQUARTERED         as state_headquartered,  -- always null
        ZIP                         as zip,             -- always null
        PHONE                       as phone,           -- always null
        AMOUNT_COVERED              as amount_covered,
        AMOUNT_UNCOVERED            as amount_uncovered,
        REVENUE                     as revenue,
        COVERED_ENCOUNTERS          as covered_encounters,
        UNCOVERED_ENCOUNTERS        as uncovered_encounters,
        COVERED_MEDICATIONS         as covered_medications,
        UNCOVERED_MEDICATIONS       as uncovered_medications,
        COVERED_PROCEDURES          as covered_procedures,
        UNCOVERED_PROCEDURES        as uncovered_procedures,
        COVERED_IMMUNIZATIONS       as covered_immunizations,
        UNCOVERED_IMMUNIZATIONS     as uncovered_immunizations,
        UNIQUE_CUSTOMERS            as unique_customers,
        QOLS_AVG                    as qols_avg,
        MEMBER_MONTHS               as member_months

    from source

)

select * from renamed
