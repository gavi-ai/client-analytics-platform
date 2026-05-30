-- models/staging/stg_stripe_payments.sql
-- Cleans and casts raw Stripe data from BigQuery raw dataset.
-- One row per charge. No business logic here — that lives in marts.

with source as (

    select * from {{ source('raw', 'stripe_raw') }}

),

renamed as (

    select
        payment_id,
        customer_id,

        -- amounts are already converted from cents in the connector
        cast(amount as numeric)          as amount,
        cast(amount_refunded as numeric) as amount_refunded,

        upper(currency)                  as currency,
        lower(status)                    as status,

        cast(paid as boolean)            as is_paid,
        cast(refunded as boolean)        as is_refunded,

        description,
        receipt_email,
        failure_code,
        failure_message,

        -- convert Unix timestamp to datetime
        timestamp_seconds(cast(created_at as int64)) as created_at,

        _partition_date,
        _loaded_at

    from source

),

deduplicated as (

    -- keep the latest load of each payment_id (handles reprocessing)
    select *
    from renamed
    qualify row_number() over (
        partition by payment_id
        order by _loaded_at desc
    ) = 1

)

select * from deduplicated
