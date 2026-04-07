-- Checks that enriched order items are not stale (older than 30 days).
-- Disabled by default so production jobs pass. Enable for demos only:
--   dbt Cloud Development: add job or env var  enable_demo_stale_items_test: true
--   CLI: dbt build --vars '{"enable_demo_stale_items_test": true}'
--
-- On this seed data (2024 orders) the test fails when enabled. Use that to show
-- failure triage and retry with: dbt build --select state:modified+

{{ config(enabled=var('enable_demo_stale_items_test', false)) }}

select
    order_item_id,
    ordered_at
from {{ ref('int_order_items_enriched') }}
where ordered_at < dateadd(day, -30, current_date)
