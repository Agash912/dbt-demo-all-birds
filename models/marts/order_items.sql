-- One row per order line item with product and order details.
-- Thin pass-through from the intermediate enrichment layer.
-- Kept as a separate mart so BI tools and reverse ETL can point here directly.

select * from {{ ref('int_order_items_enriched') }}
