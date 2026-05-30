-- models/marts/mart_revenue.sql
-- Daily revenue summary used by the Streamlit dashboard.
-- Joins Stripe payments with HubSpot customer data.
-- Incremental: only processes new partitions on each run.

{{ config(
    materialized='incremental',
    unique_key='revenue_key',
    on_schema_change='sync_all_columns',
    partition_by={
        'field': 'payment_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by=['currency', 'status']
) }}

with payments as (

    select * from {{ ref('stg_stripe_payments') }}

    {% if is_incremental() %}
    where _partition_date > (
        select max(_partition_date) from {{ this }}
    )
    {% endif %}

),

customers as (

    select
        customer_id,
        email,
        acquisition_channel,
        country,
        lifecycle_stage
    from {{ ref('stg_hubspot_contacts') }}

),

joined as (

    select
        p.payment_id,
        p.customer_id,
        c.email,
        c.acquisition_channel,
        c.country,
        c.lifecycle_stage,

        date(p.created_at)             as payment_date,
        p.currency,
        p.status,
        p.is_paid,
        p.is_refunded,

        p.amount,
        p.amount_refunded,
        p.amount - p.amount_refunded   as net_revenue,

        -- MRR approximation: monthly recurring revenue from successful charges
        case
            when p.status = 'succeeded' and not p.is_refunded
            then p.amount
            else 0
        end                            as mrr_contribution,

        p.created_at,
        p._partition_date

    from payments p
    left join customers c using (customer_id)

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['payment_id']) }} as revenue_key,
        *
    from joined

)

select * from final
