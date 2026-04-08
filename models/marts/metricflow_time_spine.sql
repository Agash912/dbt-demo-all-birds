-- Required by MetricFlow. Generates one row per day from Jan 1, 2020 to Dec 31, 2030.
-- MetricFlow joins metrics to this spine for time-series aggregation.

{{
    config(
        materialized='table'
    )
}}

with days as (

    {{
        dbt.date_spine(
            datepart="day",
            start_date="cast('2020-01-01' as date)",
            end_date="cast('2030-12-31' as date)"
        )
    }}

),

final as (

    select cast(date_day as date) as date_day
    from days

)

select * from final
