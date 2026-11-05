-- Composite key (encounter_month, encounter_class) must be unique.
select
    encounter_month,
    encounter_class,
    count(*) as row_count
from {{ ref('agg_utilization_trends') }}
group by 1, 2
having count(*) > 1
