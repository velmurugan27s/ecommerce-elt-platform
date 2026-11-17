SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp

FROM {{ source('ecommerce_staging', 'stg_reviews') }}

WHERE _batch_id = '{{ var("batch_id", "fbef6d371daf207bb5e7") }}'