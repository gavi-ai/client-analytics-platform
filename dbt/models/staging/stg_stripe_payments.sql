{{ config(materialized='view') }}

with raw_stripe as (
    select * from {{ source('raw_data', 'stripe_charges') }}
)

select
    cast(id as string) as payment_id,
    cast(amount as numeric) / 100.0 as amount, -- Converting cents to dollars
    cast(currency as string) as currency,
    cast(status as string) as payment_status,
    cast(created as timestamp) as created_at,
    cast(customer as string) as customer_id
from raw_stripe
where status = 'succeeded'