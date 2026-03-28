-- models/marts/dim_date.sql
-- Gold dimension: calendar/date spine for time-based analysis
-- Covers the full Olist dataset range (2016-09-01 to 2018-10-31)

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart    = "day",
        start_date  = "cast('2016-09-01' as date)",
        end_date    = "cast('2018-11-01' as date)"
    ) }}

),

final as (

    select
        cast(date_day as date)                           as date_key,  -- PK; join on date(timestamp)
        date_day                                         as full_date,
        extract(year  from date_day)                     as year,
        extract(quarter from date_day)                   as quarter,
        extract(month from date_day)                     as month,
        format_date('%B', date_day)                      as month_name,
        extract(week from date_day)                      as week_of_year,
        extract(dayofweek from date_day)                 as day_of_week,  -- 1=Sun in BigQuery
        format_date('%A', date_day)                      as day_name,
        extract(day from date_day)                       as day_of_month,
        case
            when extract(dayofweek from date_day) in (1, 7) then true
            else false
        end                                              as is_weekend

    from date_spine

)

select * from final
