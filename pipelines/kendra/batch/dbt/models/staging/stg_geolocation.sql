-- models/staging/stg_geolocation.sql
-- Staging layer: deduplicate geolocation by zip_code_prefix
-- Raw data has ~1M rows with duplicate prefixes; this reduces to ~5,000 unique prefixes.
-- Strategy: keep the row with the lowest latitude per prefix (deterministic tie-break).

with source as (

    select * from {{ source('olist_raw', 'geolocation') }}

),

deduped as (

    select
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state,

        -- Assign row number partitioned by zip prefix, ordered by lat for determinism
        row_number() over (
            partition by geolocation_zip_code_prefix
            order by geolocation_lat
        ) as rn

    from source

),

unique_prefixes as (

    select
        geolocation_zip_code_prefix as zip_code_prefix,
        geolocation_lat             as lat,
        geolocation_lng             as lng,
        geolocation_city            as city,
        geolocation_state           as state

    from deduped
    where rn = 1

)

select * from unique_prefixes
