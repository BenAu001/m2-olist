-- models/marts/dim_customers.sql
-- Gold dimension: one row per unique customer
-- Enriched with lat/lng from deduplicated geolocation staging model

with customers as (

    select * from {{ ref('stg_customers') }}

),

geolocation as (

    select * from {{ ref('stg_geolocation') }}

),

final as (

    select
        -- Surrogate key (BigQuery-safe)
        {{ dbt_utils.generate_surrogate_key(['c.customer_unique_id']) }} as customer_key,

        c.customer_unique_id,
        c.customer_id,           -- retain for FK joins to Fact_Orders
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,

        -- Geo coordinates from deduplicated Silver geolocation
        g.lat  as customer_lat,
        g.lng  as customer_lng

    from customers c
    left join geolocation g
        on c.customer_zip_code_prefix = g.zip_code_prefix

)

select * from final
