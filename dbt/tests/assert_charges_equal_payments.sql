-- Charges and payments balance at the total level (146,698,193.10 each).
-- Returns a row -- and therefore fails -- if they ever diverge.
select *
from (
    select
        sum(charge_amount) as total_charge,
        sum(paid_amount)   as total_paid
    from {{ ref('fct_claims_transactions') }}
)
where total_charge <> total_paid
