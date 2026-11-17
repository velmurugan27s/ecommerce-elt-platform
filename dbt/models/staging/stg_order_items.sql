SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value

FROM {{ source('ecommerce_staging', 'stg_order_items') }}

WHERE _batch_id = '{{ var("batch_id", "fbef6d371daf207bb5e7") }}'