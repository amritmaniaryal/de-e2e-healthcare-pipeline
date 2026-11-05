-- The aggregate must account for every claim. A silently dropped join
-- (a left join turned inner, a filtered-out null month) shows up here and
-- nowhere else.
select *
from (
    select
        (select sum(claim_count) from {{ ref('agg_claims_by_provider') }}) as agg_claims,
        (select count(*)         from {{ ref('stg_claims') }})             as source_claims
)
where agg_claims <> source_claims
