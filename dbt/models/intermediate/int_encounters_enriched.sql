{#
    Intermediate: int_encounters_enriched
    -------------------------------------
    GRAIN: one row per encounter (57,592 rows).

    stg_encounters, enriched with:
      * provider name and specialty      (encounters.provider_id     -> providers)
      * organisation name                (encounters.organization_id -> organizations)
      * patient demographics             (encounters.patient_id      -> patients)
      * procedure count and total cost, rolled up per encounter_id

    All four joins are LEFT.

    The three dimension joins were orphan-checked and return zero dangling
    references, so INNER would produce the same rows today. LEFT is still the
    safer default: if a dimension row ever goes missing, a LEFT join surfaces it
    as a null, whereas an INNER join silently drops the encounter -- and a
    missing row is far harder to notice than a null.

    The procedure rollup must be LEFT for a stronger reason: not every encounter
    has a procedure, and those encounters must keep their row with zero counts.

    Demographics are carried here (not just in a dim) because utilisation trends
    are usually sliced by them -- by gender, by race, by age band.
#}

with encounters as (

    select * from {{ ref('stg_encounters') }}

),

providers as (

    select
        provider_id,
        name        as provider_name,
        specialty   as provider_specialty
    from {{ ref('stg_providers') }}

),

organizations as (

    select
        organization_id,
        name        as organization_name
    from {{ ref('stg_organizations') }}

),

patients as (

    select
        patient_id,
        gender      as patient_gender,
        race        as patient_race,
        ethnicity   as patient_ethnicity,
        birth_date  as patient_birth_date,
        death_date  as patient_death_date
    from {{ ref('stg_patients') }}

),

procedures as (

    select
        encounter_id,
        count(*)        as procedure_count,
        sum(base_cost)  as total_procedure_cost
    from {{ ref('stg_procedures') }}
    group by encounter_id

),

enriched as (

    select
        -- keys
        e.encounter_id,
        e.patient_id,
        e.provider_id,
        e.organization_id,
        e.payer_id,

        -- when
        e.started_at,
        e.stopped_at,

        -- what kind of visit
        e.encounter_class,
        e.encounter_code,
        e.description,

        -- money from the source
        e.base_encounter_cost,
        e.total_claim_cost,
        e.payer_coverage,

        -- reason (null for 20,881 encounters, which is expected)
        e.reason_code,
        e.reason_description,

        -- provider context
        pr.provider_name,
        pr.provider_specialty,

        -- organisation context
        o.organization_name,

        -- patient context
        pa.patient_gender,
        pa.patient_race,
        pa.patient_ethnicity,
        pa.patient_birth_date,
        pa.patient_death_date,

        -- procedure rollup; coalesced so "no procedures" reads as zero
        coalesce(pr_count.procedure_count, 0)       as procedure_count,
        coalesce(pr_count.total_procedure_cost, 0)  as total_procedure_cost

    from encounters e
    left join providers pr            on e.provider_id = pr.provider_id
    left join organizations o         on e.organization_id = o.organization_id
    left join patients pa             on e.patient_id = pa.patient_id
    left join procedures pr_count     on e.encounter_id = pr_count.encounter_id

)

select * from enriched
