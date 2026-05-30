-- tests/assert_mrr_non_negative.sql
-- Custom dbt test: fails if any row in mart_revenue has a negative mrr_contribution.
-- Negative MRR would indicate a data bug (refund > original charge).
-- dbt tests return rows that FAIL the assertion — zero rows = test passes.

select
    payment_id,
    mrr_contribution,
    amount,
    amount_refunded
from {{ ref('mart_revenue') }}
where mrr_contribution < 0
