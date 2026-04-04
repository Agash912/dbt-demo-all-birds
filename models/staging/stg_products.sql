-- One row per product SKU. Converts price to dollars and
-- flags whether the product is a food or drink item.

with source as (

    select * from {{ source('ecom', 'raw_products') }}

),

renamed as (

    select
        sku as product_id,
        name as product_name,
        type as product_type,
        description as product_description,
        {{ cents_to_dollars('price') }} as product_price,

        -- Boolean flags for item type
        coalesce(type = 'jaffle', false) as is_food_item,
        coalesce(type = 'beverage', false) as is_drink_item

    from source

)

select * from renamed
