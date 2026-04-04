-- Joins order items to products, orders, and supply costs.
-- One row per line item with all the context needed for downstream aggregation.
--
-- This is the kind of logic that would normally live inside a stored procedure
-- as a giant multi-join query. Breaking it into a named, tested, documented
-- model makes it reusable and easy to debug.

with

order_items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

-- Aggregate supply costs to the product level
-- (one product can have multiple supply components)
supply_costs as (

    select
        product_id,
        sum(supply_cost) as product_supply_cost
    from {{ ref('stg_supplies') }}
    group by 1

),

joined as (

    select
        order_items.order_item_id,
        order_items.order_id,
        order_items.product_id,

        orders.customer_id,
        orders.ordered_at,
        orders.location_id,

        products.product_name,
        products.product_price,
        products.is_food_item,
        products.is_drink_item,

        coalesce(supply_costs.product_supply_cost, 0) as product_supply_cost

    from order_items

    left join orders on order_items.order_id = orders.order_id
    left join products on order_items.product_id = products.product_id
    left join supply_costs on order_items.product_id = supply_costs.product_id

)

select * from joined
