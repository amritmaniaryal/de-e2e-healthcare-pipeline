{#
    Staging: patients
    -----------------
    One row per patient, 1,096 rows.

    Columns are in SOURCE ORDER, so this list can be diffed line-for-line
    against `select * from healthcare.raw.patients`. That makes a dropped or
    mistyped column obvious at a glance.

    Legitimately null (8 cols): SUFFIX 1,084, DEATHDATE 1,000 (most patients
    are alive), MAIDEN 829, MARITAL 429, PASSPORT 302, PREFIX 278, DRIVERS 245,
    MIDDLE 236. Do not blanket-test these with not_null.
#}

with source as (

    select * from {{ source('raw', 'patients') }}

),

renamed as (

    select
        Id                      as patient_id,        -- primary key
        BIRTHDATE               as birth_date,
        DEATHDATE               as death_date,
        SSN                     as ssn,
        DRIVERS                 as drivers_license,
        PASSPORT                as passport,
        PREFIX                  as name_prefix,
        FIRST                   as first_name,
        MIDDLE                  as middle_name,
        LAST                    as last_name,
        SUFFIX                  as name_suffix,
        MAIDEN                  as maiden_name,
        MARITAL                 as marital_status,
        RACE                    as race,
        ETHNICITY               as ethnicity,
        GENDER                  as gender,
        BIRTHPLACE              as birth_place,
        ADDRESS                 as address,
        CITY                    as city,
        STATE                   as state,
        COUNTY                  as county,
        FIPS                    as fips,
        ZIP                     as zip,
        LAT                     as lat,
        LON                     as lon,
        HEALTHCARE_EXPENSES     as healthcare_expenses,
        HEALTHCARE_COVERAGE     as healthcare_coverage,
        INCOME                  as income

    from source

)

select * from renamed
