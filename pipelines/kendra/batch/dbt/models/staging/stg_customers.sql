-- models/staging/stg_customers.sql
-- Staging layer: clean and rename raw customers data from Bronze/GCS

with source as (

    select * from {{ source('olist_raw', 'customers') }}

),

renamed as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state

    from source

)

select * from renamed
