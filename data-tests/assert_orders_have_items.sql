-- Paid orders should have at least one line item.
-- Zero-dollar orders often have no rows in items (comps, placeholders); exclude them.
-- Severity warn: surfaces real orphan orders without failing the job.

{{ config(severity='warn') }}

select
    o.order_id
from {{ ref('orders') }} o
left join {{ ref('order_items') }} oi on o.order_id = oi.order_id
where oi.order_item_id is null
  and coalesce(o.order_total, 0) > 0
