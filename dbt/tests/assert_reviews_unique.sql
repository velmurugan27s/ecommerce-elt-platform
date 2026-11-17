SELECT
    review_id,
    order_id,
    COUNT(*) AS record_count

FROM {{ ref('fact_reviews') }}

GROUP BY
    review_id,
    order_id

HAVING COUNT(*) > 1