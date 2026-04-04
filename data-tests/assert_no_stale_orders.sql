-- Flags orders older than 90 days that have not been updated.
-- In a real pipeline, this catches tables that stop refreshing silently.
--
-- INTENTIONALLY FAILS on this dataset to demonstrate
-- what a test failure looks like in dbt Cloud and how to triage it.

select
    order_id,
    ordered_at
from {{ ref('orders') }}
where ordered_at < dateadd(day, -90, current_date)
