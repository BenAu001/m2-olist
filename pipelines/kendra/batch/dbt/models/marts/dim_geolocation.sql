-- models/marts/dim_geolocation.sql
-- Gold dimension: ~5,000 unique zip code prefixes with coordinates
-- This is the deduplicated reference table for all geographic joins

with geolocation as (

    select * from {{ ref('stg_geolocation') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['zip_code_prefix']) }} as geolocation_key,

        zip_code_prefix,
        city,
        state,
        lat,
        lng

    from geolocation

)

select * from final
