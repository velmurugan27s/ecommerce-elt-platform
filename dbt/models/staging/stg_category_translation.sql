SELECT
    product_category_name,
    product_category_name_english

FROM {{ source('ecommerce_staging', 'stg_category_translation') }}

WHERE _batch_id = '{{ var("batch_id", "fbef6d371daf207bb5e7") }}'