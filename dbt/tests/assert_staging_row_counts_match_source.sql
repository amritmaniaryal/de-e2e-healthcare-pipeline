-- Every staging model must return exactly as many rows as its source table.
-- Staging is a 1:1 rename layer, so a stray `where` clause or an accidental
-- join shows up here and nowhere else -- the key tests would still pass, and
-- the marts would just have quietly smaller numbers.

with counts as (

    select 'patients'           as table_name,
           (select count(*) from {{ ref('stg_patients') }})              as stg_rows,
           (select count(*) from {{ source('raw', 'patients') }})        as raw_rows
    union all
    select 'providers',
           (select count(*) from {{ ref('stg_providers') }}),
           (select count(*) from {{ source('raw', 'providers') }})
    union all
    select 'organizations',
           (select count(*) from {{ ref('stg_organizations') }}),
           (select count(*) from {{ source('raw', 'organizations') }})
    union all
    select 'payers',
           (select count(*) from {{ ref('stg_payers') }}),
           (select count(*) from {{ source('raw', 'payers') }})
    union all
    select 'payer_transitions',
           (select count(*) from {{ ref('stg_payer_transitions') }}),
           (select count(*) from {{ source('raw', 'payer_transitions') }})
    union all
    select 'conditions',
           (select count(*) from {{ ref('stg_conditions') }}),
           (select count(*) from {{ source('raw', 'conditions') }})
    union all
    select 'medications',
           (select count(*) from {{ ref('stg_medications') }}),
           (select count(*) from {{ source('raw', 'medications') }})
    union all
    select 'encounters',
           (select count(*) from {{ ref('stg_encounters') }}),
           (select count(*) from {{ source('raw', 'encounters') }})
    union all
    select 'procedures',
           (select count(*) from {{ ref('stg_procedures') }}),
           (select count(*) from {{ source('raw', 'procedures') }})
    union all
    select 'claims',
           (select count(*) from {{ ref('stg_claims') }}),
           (select count(*) from {{ source('raw', 'claims') }})
    union all
    select 'claims_transactions',
           (select count(*) from {{ ref('stg_claims_transactions') }}),
           (select count(*) from {{ source('raw', 'claims_transactions') }})

)

select *
from counts
where stg_rows <> raw_rows
