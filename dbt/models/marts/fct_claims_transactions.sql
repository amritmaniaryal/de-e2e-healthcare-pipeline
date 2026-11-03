{#
    Mart: fct_claims_transactions
    -------------------
    Grain: one row per claim line. All 902,713 rows.
    Key: claim_transaction_id.

#}

with claims_transactions as (
    select * from {{ ref('stg_claims_transactions') }}
),

claims as (
    select * from {{ ref('stg_claims') }}
),

final as (
    select
    -- keys
        ct.claim_transaction_id,
        ct.claim_id,
        ct.patient_id,
        ct.provider_id,
        ct.procedure_code,
        ct.place_of_service_id,

        -- the rest from the claim_transactions table, with some renames and derived columns
        ct.transaction_type,
        ct.from_date,
        ct.to_date,
        ct.units,
        ct.unit_amount,

        -- Measures are mutually exclusive by type: amount is null on PAYMENT and
        -- TRANSFEROUT rows, payments is 0 elsewhere. Never sum `amount` across all
        -- types -- TRANSFERIN is money moving between payers, not new spend.
        case when ct.transaction_type = 'CHARGE'     then ct.amount   end as charge_amount,
        case when ct.transaction_type = 'PAYMENT'    then ct.payments end as paid_amount,
        case when ct.transaction_type = 'TRANSFERIN' then ct.amount   end as transfer_in_amount,

        --only one column from the claims table, but it is important for time-based analysis
        c.service_date
    from claims_transactions ct

    -- left join so a broken claim linkage shows as a null service_date (fails the
    -- not_null test) instead of silently dropping transactions
    left join claims c
        on ct.claim_id = c.claim_id
)

select * from final