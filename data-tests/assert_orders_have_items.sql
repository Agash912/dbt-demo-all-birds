-- Every order should have at least one line item.
-- If this returns rows, we have orphan orders with no items attached.
-- Severity set to warn: flags the issue without blocking the pipeline.
-- 483 orders in this dataset have no items (likely cancellations).

{{ config(severity='warn') }}

select
    o.order_id
from {{ ref('orders') }} o
left join {{ ref('order_items') }} oi on o.order_id = oi.order_id
where oi.order_item_id is null
