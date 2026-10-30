with patients as ( select * from {{ ref('stg_patients') }} ),

final as (
    select
        patient_id,
        gender, race, ethnicity, marital_status, 
        birth_date, death_date, 

        -- Derived fields (date related)
        datediff('year', birth_date, date '2026-09-26') as age,
        case
            when age between 0 and 17 then '0-17'
            when age between 18 and 34 then '18-34'
            when age between 35 and 49 then '35-49'
            when age between 50 and 64 then '50-64'
            when age >= 65 then '65+'
            when age is null then 'Unknown'
        end as age_band,
        (death_date is not null) as is_deceased,

        -- income fields (original and derived)
        income,
        case
            when income < 25000 then '<25k'
            when income < 50000 then '25k-50k'
            when income < 75000 then '50k-75k'
            when income < 100000 then '75k-100k'
            when income >= 100000 then '100k+'
            when income is null then 'Unknown'
        end as income_band,
        -- income band order
        case
            when income < 25000 then 1
            when income < 50000 then 2
            when income < 75000 then 3
            when income < 100000 then 4
            when income >= 100000 then 5
            else 0
        end as income_band_order,

        --geography fields
        -- NOTE: state, county and city are constant in the current dataset -- every
        -- patient is in Charlotte, NC, because of how Synthea was invoked. They are
        -- kept because they are genuine domain attributes; regenerate with multiple
        -- cities (-c) and they become useful immediately, with no model changes.

        state, county, city, zip
        from patients
)

select * from final
