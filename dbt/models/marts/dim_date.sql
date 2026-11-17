WITH all_dates AS (

    SELECT order_purchase_timestamp::DATE AS date_day
    FROM {{ ref('stg_orders') }}

    UNION ALL

    SELECT order_approved_at::DATE
    FROM {{ ref('stg_orders') }}

    UNION ALL

    SELECT order_delivered_carrier_date::DATE
    FROM {{ ref('stg_orders') }}

    UNION ALL

    SELECT order_delivered_customer_date::DATE
    FROM {{ ref('stg_orders') }}

    UNION ALL

    SELECT order_estimated_delivery_date::DATE
    FROM {{ ref('stg_orders') }}

    UNION ALL

    SELECT shipping_limit_date::DATE
    FROM {{ ref('stg_order_items') }}

    UNION ALL

    SELECT review_creation_date::DATE
    FROM {{ ref('stg_reviews') }}

    UNION ALL

    SELECT review_answer_timestamp::DATE
    FROM {{ ref('stg_reviews') }}

),

date_bounds AS (

    SELECT
        MIN(date_day) AS min_date,
        MAX(date_day) AS max_date

    FROM all_dates
    WHERE date_day IS NOT NULL

),

calendar AS (

    SELECT
        generate_series(
            min_date,
            max_date,
            INTERVAL '1 day'
        )::DATE AS date_day

    FROM date_bounds

)

SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::INTEGER AS date_key,
    date_day,

    EXTRACT(YEAR FROM date_day)::INTEGER AS year,
    EXTRACT(QUARTER FROM date_day)::INTEGER AS quarter,
    EXTRACT(MONTH FROM date_day)::INTEGER AS month,
    TO_CHAR(date_day, 'FMMonth') AS month_name,

    EXTRACT(DAY FROM date_day)::INTEGER AS day_of_month,
    EXTRACT(ISODOW FROM date_day)::INTEGER AS day_of_week,
    TO_CHAR(date_day, 'FMDay') AS day_name,

    EXTRACT(WEEK FROM date_day)::INTEGER AS week_of_year,
    EXTRACT(ISOYEAR FROM date_day)::INTEGER AS iso_year,

    CASE
        WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7)
            THEN TRUE
        ELSE FALSE
    END AS is_weekend

FROM calendar
