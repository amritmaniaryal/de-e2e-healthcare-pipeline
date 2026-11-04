{#
    Mart: agg_cost_per_procedure
    ----------------------------
    Grain: one row per procedure_code (674 codes).

    Source: fct_claims_transactions, which holds all four transaction types in
    one table. Measures are computed with conditional counts/sums so each one
    reads only its own type -- charge_amount is null outside CHARGE rows, so
    count(charge_amount) counts charges only.

    procedure_code is a STRING and must stay one: it holds SNOMED codes and
    two-digit values like "03" whose leading zero a cast would destroy.

    avg_charge_per_unit guards against division by zero with nullif(): a code
    could in principle have a charge row with zero units.
#}

with transactions as (

    select * from {{ ref('fct_claims_transactions') }}

),

aggregated as (

    select
        procedure_code,
        count(charge_amount)                                    as charge_count,
        count(paid_amount)                                      as payment_count,
        sum(charge_amount)                                      as total_charge,
        sum(paid_amount)                                        as total_paid,
        sum(case when charge_amount is not null then units end) as total_units,
        avg(charge_amount)                                      as avg_charge_per_line,
        sum(charge_amount)
            / nullif(sum(case when charge_amount is not null then units end), 0)
                                                                as avg_charge_per_unit
    from transactions
    group by 1

),

final as (

    select
        procedure_code,
        charge_count,
        payment_count,
        total_charge,
        total_paid,
        total_units,
        avg_charge_per_line,
        avg_charge_per_unit,
        -- charges and payments balance at the total level; per code they can
        -- differ where a code was charged but not yet fully paid
        total_charge - coalesce(total_paid, 0) as charge_payment_gap
    from aggregated

)

select * from final
