-- Checks that enriched order items are not stale (older than 30 days).
-- If this returns rows, the intermediate layer has data that hasn't been
-- refreshed recently, which means something upstream may have stopped loading.
--
-- INTENTIONALLY FAILS on this dataset because the sample data is from 2024.
-- Used during the demo to show:
--   1. What a test failure looks like in dbt Cloud
--   2. How to triage and identify the root cause
--   3. How to re-run only the affected models with: dbt build --select state:modified+

select
    order_item_id,
    ordered_at
from {{ ref('int_order_items_enriched') }}
where ordered_at < dateadd(day, -30, current_date)
