SELECT
    p.order_id,
    p.payment_sequential,

    o.customer_id,

    TO_CHAR(o.order_purchase_timestamp::DATE, 'YYYYMMDD')::INTEGER
        AS order_purchase_date_key,

    o.order_status,

    p.payment_type,
    p.payment_installments,
    p.payment_value,

    o.order_purchase_timestamp

FROM {{ ref('stg_payments') }} p

LEFT JOIN {{ ref('stg_orders') }} o
    ON p.order_id = o.order_id

LEFT JOIN {{ ref('dim_customers') }} dc
    ON o.customer_id = dc.customer_id
