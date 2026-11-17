SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value

FROM {{ source('ecommerce_staging', 'stg_payments') }}

WHERE _batch_id = '{{ var("batch_id", "fbef6d371daf207bb5e7") }}'