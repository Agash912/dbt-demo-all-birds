-- One row per order. Renames columns, converts cents to dollars,
-- and truncates the timestamp to date grain.

with source as (

    select * from {{ source('ecom', 'raw_orders') }}

),

renamed as (

    select
        id as order_id,
        store_id as location_id,
        customer as customer_id,

        -- Convert cents to dollars using the project macro
        {{ cents_to_dollars('subtotal') }} as subtotal,
        {{ cents_to_dollars('tax_paid') }} as tax_paid,
        {{ cents_to_dollars('order_total') }} as order_total,

        {{ dbt.date_trunc('day', 'ordered_at') }} as ordered_at

    from source

)

select * from renamed
