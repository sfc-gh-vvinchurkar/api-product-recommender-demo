{{ config(materialized='view', schema='STAGING') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_transactions') }}
)

SELECT
    transaction_id,
    member_id,
    product_id,
    store_id,
    store_name,
    transaction_date,
    transaction_time,
    DATE_TRUNC('month', transaction_date) AS transaction_month,
    DATE_TRUNC('week', transaction_date) AS transaction_week,
    DAYOFWEEK(transaction_date) AS day_of_week,
    CASE WHEN DAYOFWEEK(transaction_date) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    quantity,
    unit_price,
    discount_amount,
    total_amount,
    points_earned,
    points_redeemed,
    payment_method,
    channel,
    basket_id,
    _ingested_at
FROM source
