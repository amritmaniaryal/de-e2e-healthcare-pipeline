{#
    Mart: agg_utilization_trends
    ----------------------------
    Grain: one row per encounter month per encounter class.

    Source: int_encounters_enriched, which already carries the procedure rollup
    per encounter, so no join is needed here.

    unique_patients uses count(distinct patient_id). Do NOT sum this column
    across months -- a patient seen in two months is counted in both, so the
    monthly values deliberately overstate the annual total.

    Encounter months span 1927-2026 because Synthea backdates visits, so a
    trend chart over the full range is mostly flat with detail concentrated
    near the present.
#}

with encounters as (

    select * from {{ ref('int_encounters_enriched') }}

),

aggregated as (

    select
        date_trunc('month', started_at)     as encounter_month,
        encounter_class,
        count(*)                            as encounter_count,
        count(distinct patient_id)          as unique_patients,
        sum(total_claim_cost)               as total_claim_cost,
        sum(payer_coverage)                 as total_payer_coverage,
        sum(procedure_count)                as total_procedures,
        sum(total_procedure_cost)           as total_procedure_cost
    from encounters
    group by 1, 2

),

final as (

    select
        encounter_month,
        encounter_class,
        encounter_count,
        unique_patients,
        total_claim_cost,
        total_payer_coverage,
        total_procedures,
        total_procedure_cost
    from aggregated

)

select * from final
