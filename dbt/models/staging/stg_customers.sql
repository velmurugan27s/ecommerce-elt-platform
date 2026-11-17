SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state

FROM {{ source('ecommerce_staging', 'stg_customers') }}

WHERE _batch_id = '{{ var("batch_id", "fbef6d371daf207bb5e7") }}'