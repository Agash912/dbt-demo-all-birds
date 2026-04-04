-- One row per customer. Combines profile info with lifetime order metrics
-- and the segment assigned by the Python model.

with

customers as (

    select * from {{ ref('stg_customers') }}

),

segments as (

    select * from {{ ref('int_customer_segments') }}

),

final as (

    select
        customers.customer_id,
        customers.customer_name,

        segments.order_count,
        segments.first_ordered_at,
        segments.last_ordered_at,
        segments.lifetime_subtotal,
        segments.lifetime_tax,
        segments.lifetime_spend,
        segments.avg_order_value,
        segments.customer_segment,

        case
            when segments.order_count > 1 then 'returning'
            else 'new'
        end as customer_type

    from customers
    left join segments on customers.customer_id = segments.customer_id

)

select * from final
