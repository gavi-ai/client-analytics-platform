-- If this query returns ANY rows, the pipeline will fail and send a Slack alert.
-- MRR should never be less than 0.

with mrr_data as (
    select * from {{ ref('mart_revenue') }}
)

select
    customer_id,
    total_mrr
from mrr_data
where total_mrr < 0