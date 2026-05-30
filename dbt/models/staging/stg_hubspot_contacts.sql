-- models/staging/stg_hubspot_contacts.sql
-- Cleans and standardises raw HubSpot contact data.
-- One row per customer_id. SCD Type 2 history tracked in dim_customers_scd2.

with source as (

    select * from {{ source('raw', 'hubspot_raw') }}

),

renamed as (

    select
        customer_id,
        lower(trim(email))             as email,
        trim(first_name)               as first_name,
        trim(last_name)                as last_name,
        phone,
        company,

        lower(lifecycle_stage)         as lifecycle_stage,
        lower(lead_status)             as lead_status,
        lower(country)                 as country,
        lower(city)                    as city,

        -- normalise acquisition channels to consistent names
        case lower(acquisition_channel)
            when 'organic_search'     then 'organic_search'
            when 'paid_search'        then 'paid_search'
            when 'social'             then 'social_media'
            when 'email_marketing'    then 'email'
            when 'direct_traffic'     then 'direct'
            when 'referrals'          then 'referral'
            else 'other'
        end                            as acquisition_channel,

        owner_id,

        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', created_at) as created_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', updated_at) as updated_at,

        _partition_date,
        _loaded_at

    from source

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by customer_id
        order by updated_at desc nulls last
    ) = 1

)

select * from deduplicated
