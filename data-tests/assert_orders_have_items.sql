-- Every order should have at least one line item.
-- If this returns rows, we have orphan orders with no items attached.
-- Expected result: PASS (no orphan orders in this dataset)

select
    o.order_id
from {{ ref('orders') }} o
left join {{ ref('order_items') }} oi on o.order_id = oi.order_id
where oi.order_item_id is null
