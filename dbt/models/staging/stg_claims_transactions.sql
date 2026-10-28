{#
    Staging: claims_transactions
    ----------------------------
    902,713 rows. The largest table. Columns are in SOURCE ORDER (33 columns).

    THE PRIMARY KEY COLUMN IS `ID`, UPPERCASE. Every other table in this
    project uses `Id`. Renamed here to claim_transaction_id.

    PROCEDURECODE is a STRING upstream and must stay one: the column contains
    both SNOMED codes (410620009) and two-digit values with a leading zero
    ("03", "08"). Casting to a number silently destroys those zeros.

    IMPORTANT NULLS, and they are structural rather than defects -- a row is
    only one TYPE of transaction, so the fields of the other types are null:
        MODIFIER1, MODIFIER2, LINENOTE   902,713  -- blank in EVERY row
        DIAGNOSISREF4                    875,163
        DIAGNOSISREF3                    805,953
        TRANSFEROUTID                    750,137
        TRANSFERS                        597,561
        DIAGNOSISREF2                    591,303
        METHOD                           570,945
        AMOUNT / TRANSFERTYPE            484,344  -- null when not a payment row
        PATIENTINSURANCEID                44,157

    UNVERIFIED FOREIGN KEY: PLACEOFSERVICE has no _id suffix but is probably an
    organization id. Confirm with an orphan check before joining.
#}

with source as (

    select * from {{ source('raw', 'claims_transactions') }}

),

renamed as (

    select
        ID                      as claim_transaction_id,  -- PK, uppercase in source
        CLAIMID                 as claim_id,              -- foreign key -> claims
        CHARGEID                as charge_id,
        PATIENTID               as patient_id,            -- foreign key -> patients
        TYPE                    as transaction_type,
        AMOUNT                  as amount,
        METHOD                  as payment_method,
        FROMDATE                as from_date,
        TODATE                  as to_date,
        PLACEOFSERVICE          as place_of_service_id,
        PROCEDURECODE           as procedure_code,        -- string, leading zeros matter
        MODIFIER1               as modifier_1,            -- always null
        MODIFIER2               as modifier_2,            -- always null
        DIAGNOSISREF1           as diagnosis_ref_1,
        DIAGNOSISREF2           as diagnosis_ref_2,
        DIAGNOSISREF3           as diagnosis_ref_3,
        DIAGNOSISREF4           as diagnosis_ref_4,
        UNITS                   as units,                 -- a count, NOT a key
        DEPARTMENTID            as department_id,         -- no departments table
        NOTES                   as notes,
        UNITAMOUNT              as unit_amount,
        TRANSFEROUTID           as transfer_out_id,
        TRANSFERTYPE            as transfer_type,
        PAYMENTS                as payments,
        ADJUSTMENTS             as adjustments,
        TRANSFERS               as transfers,
        OUTSTANDING             as outstanding,
        APPOINTMENTID           as appointment_id,
        LINENOTE                as line_note,             -- always null
        PATIENTINSURANCEID      as patient_insurance_id,
        FEESCHEDULEID           as fee_schedule_id,
        PROVIDERID              as provider_id,           -- foreign key -> providers
        SUPERVISINGPROVIDERID   as supervising_provider_id

    from source

)

select * from renamed
