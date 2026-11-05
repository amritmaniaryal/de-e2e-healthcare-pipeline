-- Composite key (provider_id, service_month) must be unique. Cannot be a
-- generic `unique` test, which only accepts a single column.
select
    provider_id,
    service_month,
    count(*) as row_count
from {{ ref('agg_claims_by_provider') }}
group by 1, 2
having count(*) > 1
