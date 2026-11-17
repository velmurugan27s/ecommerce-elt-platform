WITH expected AS (

    SELECT
        'fact_order_items' AS model_name,
        COUNT(*) AS expected_count
    FROM {{ ref('stg_order_items') }}

    UNION ALL

    SELECT
        'fact_payments',
        COUNT(*)
    FROM {{ ref('stg_payments') }}

    UNION ALL

    SELECT
        'fact_reviews',
        COUNT(*)
    FROM {{ ref('stg_reviews') }}

),

actual AS (

    SELECT
        'fact_order_items' AS model_name,
        COUNT(*) AS actual_count
    FROM {{ ref('fact_order_items') }}

    UNION ALL

    SELECT
        'fact_payments',
        COUNT(*)
    FROM {{ ref('fact_payments') }}

    UNION ALL

    SELECT
        'fact_reviews',
        COUNT(*)
    FROM {{ ref('fact_reviews') }}

)

SELECT
    e.model_name,
    e.expected_count,
    a.actual_count

FROM expected e

JOIN actual a
    ON e.model_name = a.model_name

WHERE e.expected_count <> a.actual_count