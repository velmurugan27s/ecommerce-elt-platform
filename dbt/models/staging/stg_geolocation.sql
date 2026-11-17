SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state

FROM {{ source('ecommerce_staging', 'stg_geolocation') }}

WHERE _batch_id = '{{ var("batch_id", "fbef6d371daf207bb5e7") }}'