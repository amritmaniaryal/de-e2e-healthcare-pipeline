{#
    Intermediate: int_claims_enriched
    ---------------------------------
    GRAIN: one row per claim (97,394 rows).

    stg_claims, enriched with:
      * the primary payer's name and ownership (join on primary_patient_insurance_id,
        which an orphan check proved really is a payers.payer_id -- not a member id)
      * claim-line totals rolled up per claim_id

    WHY LEFT JOINS
    A claim with no claim lines must keep its row with zero totals. An INNER join
    would silently drop it, and "fewer rows than expected" is the hardest kind of
    bug to notice.

    NOT JOINED: patient_insurance_id on the transactions side. It looks like a
    reference to payer_transitions.member_id but has 465 dangling references out
    of 858,556, so joining it would quietly lose rows. Nothing useful comes across
    anyway once payer_id is available.

    Only DIAGNOSIS1 is carried forward as primary_diagnosis_code. DIAGNOSIS2-8 are
    null for 68k-97k of 97,394 rows; they stay available in stg_claims if needed.
#}

with claims as (

    select * from {{ ref('stg_claims') }}

),

payers as (

    select
        payer_id,
        name        as payer_name,
        ownership   as payer_ownership
    from {{ ref('stg_payers') }}

),

claim_lines as (

    select
        claim_id,
        count(*)            as line_count,
        -- CHARGE only: raw `amount` is also populated on TRANSFERIN rows,
        -- which is money moving between payers, not new spend
        sum(case when transaction_type = 'CHARGE' then amount end) as total_charge_amount,
        sum(payments)       as total_payments,
        sum(adjustments)    as total_adjustments,
        sum(transfers)      as total_transfers,
        sum(outstanding)    as total_outstanding,
        sum(units)          as total_units
    from {{ ref('stg_claims_transactions') }}
    group by claim_id

),

enriched as (

    select
        -- keys
        c.claim_id,
        c.patient_id,
        c.provider_id,
        c.supervising_provider_id,
        c.primary_patient_insurance_id,
        p.payer_name                as primary_payer_name,
        p.payer_ownership           as primary_payer_ownership,

        -- dates
        c.service_date,
        c.current_illness_date,

        -- attributes
        c.status_1,
        c.status_p,
        c.healthcare_claim_type_id_1,
        c.diagnosis_1               as primary_diagnosis_code,

        -- claim-level money from the source
        c.outstanding_1,
        c.outstanding_p,

        -- claim-line totals; coalesced so a claim with no lines reads as zero
        coalesce(l.line_count, 0)           as line_count,
        coalesce(l.total_charge_amount, 0)  as total_charge_amount,
        coalesce(l.total_payments, 0)       as total_payments,
        coalesce(l.total_adjustments, 0)    as total_adjustments,
        coalesce(l.total_transfers, 0)      as total_transfers,
        coalesce(l.total_outstanding, 0)    as total_outstanding,
        coalesce(l.total_units, 0)          as total_units

    from claims c
    left join payers p       on c.primary_patient_insurance_id = p.payer_id
    left join claim_lines l  on c.claim_id = l.claim_id

)

select * from enriched
