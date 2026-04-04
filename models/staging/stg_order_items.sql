-- One row per line item in an order. Links orders to product SKUs.

with source as (

    select * from {{ source('ecom', 'raw_items') }}

),

renamed as (

    select
        id as order_item_id,
        order_id,
        sku as product_id
    from source

)

select * from renamed
