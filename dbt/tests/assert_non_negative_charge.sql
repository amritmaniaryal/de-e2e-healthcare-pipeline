-- No charge should ever be negative. A negative charge_amount means the
-- CHARGE/PAYMENT split is wrong, not that a refund happened.
select
    claim_transaction_id,
    charge_amount
from {{ ref('fct_claims_transactions') }}
where charge_amount < 0
