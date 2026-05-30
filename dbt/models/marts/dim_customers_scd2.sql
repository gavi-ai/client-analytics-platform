-- models/marts/dim_customers_scd2.sql
-- Slowly Changing Dimension (Type 2) for customers.
-- Preserves full history of customer attribute changes (plan tier, lifecycle stage, etc.)
-- Each change creates a new row with valid_from / valid_to dates.
-- The current record has valid_to = null and is_current = true.

{{ config(
    materialized='incremental',
    unique_key='customer_surrogate_key',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True
) }}

with source as (

    select * from {{ ref('stg_hubspot_contacts') }}

),

-- Add a hash of all tracked columns to detect changes efficiently
with_change_hash as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key([
            'lifecycle_stage',
            'lead_status',
            'acquisition_channel',
            'country'
        ]) }} as attributes_hash

    from source

),

final as (

    select
        -- surrogate key: customer + version hash ensures uniqueness per change
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'attributes_hash']) }}
            as customer_surrogate_key,

        customer_id,
        email,
        first_name,
        last_name,
        company,
        lifecycle_stage,
        lead_status,
        acquisition_channel,
        country,
        city,
        owner_id,
        attributes_hash,

        created_at,
        updated_at              as valid_from,
        null                    as valid_to,     -- dbt snapshot fills this on change
        true                    as is_current,

        current_timestamp()     as dbt_updated_at

    from with_change_hash

)

select * from final
