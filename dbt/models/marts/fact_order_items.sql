SELECT
    oi.order_id,
    oi.order_item_id,

    o.customer_id,
    oi.product_id,
    oi.seller_id,

    TO_CHAR(o.order_purchase_timestamp::DATE, 'YYYYMMDD')::INTEGER
        AS order_purchase_date_key,

    TO_CHAR(oi.shipping_limit_date::DATE, 'YYYYMMDD')::INTEGER
        AS shipping_limit_date_key,

    o.order_status,

    oi.shipping_limit_date,
    o.order_purchase_timestamp,

    oi.price,
    oi.freight_value,

    oi.price + oi.freight_value AS item_total_amount

FROM {{ ref('stg_order_items') }} oi

LEFT JOIN {{ ref('stg_orders') }} o
    ON oi.order_id = o.order_id

LEFT JOIN {{ ref('dim_customers') }} dc
    ON o.customer_id = dc.customer_id

LEFT JOIN {{ ref('dim_products') }} dp
    ON oi.product_id = dp.product_id

LEFT JOIN {{ ref('dim_sellers') }} ds
    ON oi.seller_id = ds.seller_id
