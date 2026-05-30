{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'dbt_scd_id'],
    strategy='timestamp',
    updated_at='updated_at'
) }}

with source as (
    select * from {{ ref('stg_hubspot_contacts') }}
),

final as (
    select
        customer_id,
        email,
        first_name,
        last_name,
        plan_tier,
        acquisition_channel,
        country,
        updated_at,
        current_timestamp() as dbt_updated_at,
        true as is_current
    from source
)

select * from final