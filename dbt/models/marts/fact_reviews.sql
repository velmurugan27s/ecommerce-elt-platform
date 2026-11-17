SELECT
    r.review_id,
    r.order_id,

    o.customer_id,

    TO_CHAR(r.review_creation_date::DATE, 'YYYYMMDD')::INTEGER
        AS review_creation_date_key,

    TO_CHAR(r.review_answer_timestamp::DATE, 'YYYYMMDD')::INTEGER
        AS review_answer_date_key,

    r.review_score,
    r.review_comment_title,
    r.review_comment_message,

    r.review_creation_date,
    r.review_answer_timestamp,

    o.order_status

FROM {{ ref('stg_reviews') }} r

LEFT JOIN {{ ref('stg_orders') }} o
    ON r.order_id = o.order_id

LEFT JOIN {{ ref('dim_customers') }} dc
    ON o.customer_id = dc.customer_id
