-- Aggregates order data to one row per customer.
-- Produces lifetime spend, order counts, and date ranges.
--
-- Feeds into the Python segmentation model and then into the customers mart.
-- Splitting the aggregation from the segmentation makes each piece testable on its own.

with

orders as (

    select * from {{ ref('stg_orders') }}

),

summary as (

    select
        customer_id,

        count(distinct order_id) as order_count,
        min(ordered_at) as first_ordered_at,
        max(ordered_at) as last_ordered_at,

        sum(subtotal) as lifetime_subtotal,
        sum(tax_paid) as lifetime_tax,
        sum(order_total) as lifetime_spend,

        {{ safe_divide('sum(order_total)', 'count(distinct order_id)') }} as avg_order_value

    from orders
    group by 1

)

select * from summary
