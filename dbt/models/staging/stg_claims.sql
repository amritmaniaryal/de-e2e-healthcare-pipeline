{#
    Staging: claims
    ---------------
    97,394 rows. Columns are in SOURCE ORDER (31 columns).

    The numbered suffixes mean different payer portions of the same claim:
        1 = primary payer
        2 = secondary payer
        P = patient (the patient's own share)
    So STATUS1 / STATUS2 / STATUSP are three separate statuses, not versions.

    IMPORTANT NULLS (large, and expected):
        REFERRINGPROVIDERID          97,394  -- blank in every single row
        DIAGNOSIS8                   97,369
        DIAGNOSIS7                   97,214
        DIAGNOSIS6                   97,031
        DIAGNOSIS5                   96,250
        DIAGNOSIS4                   94,195
        DIAGNOSIS3                   87,238
        SECONDARYPATIENTINSURANCEID  70,923
        DIAGNOSIS2                   67,866
        STATUS2 / OUTSTANDING2 / LASTBILLEDDATE2  26,473

    DIAGNOSIS1-8 are diagnosis codes held as STRING upstream (up to 17 digits).
    OUTSTANDING1/2/P are money, decimal(12,2).

    UNVERIFIED FOREIGN KEYS: PRIMARYPATIENTINSURANCEID, SECONDARYPATIENTINSURANCEID
    and APPOINTMENTID look like references but have no obvious target table.
    Run an orphan check before joining on them.
#}

with source as (

    select * from {{ source('raw', 'claims') }}

),

renamed as (

    select
        Id                              as claim_id,        -- primary key
        PATIENTID                       as patient_id,      -- foreign key -> patients
        PROVIDERID                      as provider_id,     -- foreign key -> providers
        PRIMARYPATIENTINSURANCEID       as primary_patient_insurance_id,
        SECONDARYPATIENTINSURANCEID     as secondary_patient_insurance_id,
        DEPARTMENTID                    as department_id,   -- no departments table
        PATIENTDEPARTMENTID             as patient_department_id,
        DIAGNOSIS1                      as diagnosis_1,     -- string, up to 17 digits
        DIAGNOSIS2                      as diagnosis_2,
        DIAGNOSIS3                      as diagnosis_3,
        DIAGNOSIS4                      as diagnosis_4,
        DIAGNOSIS5                      as diagnosis_5,
        DIAGNOSIS6                      as diagnosis_6,
        DIAGNOSIS7                      as diagnosis_7,
        DIAGNOSIS8                      as diagnosis_8,
        REFERRINGPROVIDERID             as referring_provider_id,  -- always null
        APPOINTMENTID                   as appointment_id,
        CURRENTILLNESSDATE              as current_illness_date,
        SERVICEDATE                     as service_date,
        SUPERVISINGPROVIDERID           as supervising_provider_id,
        STATUS1                         as status_1,
        STATUS2                         as status_2,
        STATUSP                         as status_p,
        OUTSTANDING1                    as outstanding_1,
        OUTSTANDING2                    as outstanding_2,
        OUTSTANDINGP                    as outstanding_p,
        LASTBILLEDDATE1                 as last_billed_date_1,
        LASTBILLEDDATE2                 as last_billed_date_2,
        LASTBILLEDDATEP                 as last_billed_date_p,
        HEALTHCARECLAIMTYPEID1          as healthcare_claim_type_id_1,
        HEALTHCARECLAIMTYPEID2          as healthcare_claim_type_id_2

    from source

)

select * from renamed
