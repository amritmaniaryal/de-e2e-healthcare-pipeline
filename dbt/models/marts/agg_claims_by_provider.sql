{#
    Mart: agg_claims_by_provider
    ----------------------------
    Grain: one row per provider per service month.
    Source: int_claims_enriched (claim grain) + dim_providers (labels).

    Measures are computed first and the dimension joined after, so the join
    touches a few thousand aggregated rows rather than all 97,394 claims.

    total_charged uses int_claims_enriched.total_charge_amount, which is
    CHARGE-only. Do not substitute total_amount -- that included TRANSFERIN,
    which is money moving between payers, not new spend.

    No filter on service_date: a null month would then show up as a visible
    null group rather than silently dropping claims from the totals.
#}

with claims as (

    select * from {{ ref('int_claims_enriched') }}

),

providers as (

    select
        provider_id,
        name        as provider_name,
        specialty   as provider_specialty
    from {{ ref('dim_providers') }}

),

aggregated as (

    select
        provider_id,
        date_trunc('month', service_date)   as service_month,
        count(*)                            as claim_count,
        sum(total_charge_amount)            as total_charged,
        sum(total_payments)                 as total_paid,
        avg(total_payments)                 as avg_paid_per_claim
    from claims
    group by 1, 2

),

final as (

    select
        a.provider_id,
        p.provider_name,
        p.provider_specialty,
        a.service_month,
        a.claim_count,
        a.total_charged,
        a.total_paid,
        a.avg_paid_per_claim
    from aggregated a
    left join providers p on a.provider_id = p.provider_id

)

select * from final
