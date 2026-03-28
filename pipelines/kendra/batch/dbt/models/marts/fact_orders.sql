with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select customer_id, customer_key, customer_state, customer_zip_code_prefix
    from {{ ref('dim_customers') }}
),

geolocation as (
    select zip_code_prefix, city, state, lat, lng
    from {{ ref('dim_geolocation') }}
)

select
    -- Surrogate key for the fact row
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_key,

    -- Foreign keys to dimensions
    o.order_id,
    c.customer_key,
    cast(o.order_purchase_timestamp as date)       as order_date_key,
    cast(o.order_delivered_customer_date as date)  as delivery_date_key,

    -- Dimension attributes (degenerate / convenience denorms)
    o.order_status,
    c.customer_state,
    c.customer_zip_code_prefix,
    g.state as geo_state,
    g.city  as geo_city,

    -- Delivery time metrics (in days)
    date_diff(cast(o.order_delivered_customer_date as date),
              cast(o.order_purchase_timestamp as date), day) as actual_delivery_days,

    date_diff(cast(o.order_estimated_delivery_date as date),
              cast(o.order_purchase_timestamp as date), day) as estimated_delivery_days,

    date_diff(cast(o.order_estimated_delivery_date as date),
              cast(o.order_delivered_customer_date as date), day) as delivery_delta_days,

    -- Raw timestamps
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date

from orders o
left join customers c on o.customer_id = c.customer_id
left join geolocation g on c.customer_zip_code_prefix = g.zip_code_prefix