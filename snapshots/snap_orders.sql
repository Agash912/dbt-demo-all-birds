-- Captures a history of every change to raw_orders (SCD Type 2).
-- Each time an order's total or timestamp changes, a new row is created
-- with dbt_valid_from / dbt_valid_to columns tracking the version window.
-- Useful for auditing: "what did this order look like last Tuesday?"

{% snapshot snap_orders %}

{{
    config(
        target_schema='snapshots',
        unique_key='id',
        strategy='timestamp',
        updated_at='ordered_at'
    )
}}

select * from {{ source('ecom', 'raw_orders') }}

{% endsnapshot %}
