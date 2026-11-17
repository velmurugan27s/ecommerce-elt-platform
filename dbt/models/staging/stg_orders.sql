SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date

FROM {{ source('ecommerce_staging', 'stg_orders') }}

WHERE _batch_id = '{{ var("batch_id", "fbef6d371daf207bb5e7") }}'