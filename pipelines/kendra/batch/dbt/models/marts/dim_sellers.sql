-- models/marts/dim_sellers.sql
-- Gold dimension: one row per seller
-- Enriched with lat/lng from deduplicated geolocation staging model

with sellers as (

    select * from {{ ref('stg_sellers') }}

),

geolocation as (

    select * from {{ ref('stg_geolocation') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['s.seller_id']) }} as seller_key,

        s.seller_id,
        s.seller_zip_code_prefix,
        s.seller_city,
        s.seller_state,

        -- Geo coordinates from deduplicated Silver geolocation
        g.lat as seller_lat,
        g.lng as seller_lng

    from sellers s
    left join geolocation g
        on s.seller_zip_code_prefix = g.zip_code_prefix

)

select * from final
