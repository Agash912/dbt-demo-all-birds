-- One row per order. Incremental with merge strategy.
-- Includes a 3-day lookback for late-arriving data.
-- Contract enforced: column names and types are guaranteed.

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        strategy='merge',
        on_schema_change='append_new_columns',
        cluster_by=['ordered_at'],
        tags=['daily']
    )
}}

with

orders as (

    select * from {{ ref('stg_orders') }}

    {% if is_incremental() %}
        -- 3-day lookback catches late-arriving records.
        -- Only scans the recent window instead of the full table.
        where ordered_at >= (select dateadd(day, -3, max(ordered_at)) from {{ this }})
    {% endif %}

),

-- Aggregate line item details to the order grain
order_items_summary as (

    select
        order_id,
        sum(product_supply_cost) as order_cost,
        sum(product_price) as order_items_subtotal,
        count(*) as count_order_items,
        sum(case when is_food_item then 1 else 0 end) as count_food_items,
        sum(case when is_drink_item then 1 else 0 end) as count_drink_items

    from {{ ref('int_order_items_enriched') }}
    group by 1

),

-- Assign a sequence number so we know if it is the customer's 1st, 2nd, 3rd order
customer_order_count as (

    select
        orders.order_id,
        orders.customer_id,
        orders.location_id,
        orders.ordered_at,
        orders.subtotal,
        orders.tax_paid,
        orders.order_total,

        order_items_summary.order_cost,
        order_items_summary.order_items_subtotal,
        order_items_summary.count_order_items,
        order_items_summary.count_food_items,
        order_items_summary.count_drink_items,

        (order_items_summary.count_food_items > 0) as is_food_order,
        (order_items_summary.count_drink_items > 0) as is_drink_order,

        row_number() over (
            partition by orders.customer_id
            order by orders.ordered_at
        ) as customer_order_number

    from orders
    left join order_items_summary on orders.order_id = order_items_summary.order_id

)

select * from customer_order_count
