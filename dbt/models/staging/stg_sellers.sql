SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state

FROM {{ source('ecommerce_staging', 'stg_sellers') }}

WHERE _batch_id = '{{ var("batch_id", "fbef6d371daf207bb5e7") }}'