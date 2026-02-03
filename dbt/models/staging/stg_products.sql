{{ config(materialized='view', schema='STAGING') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_products') }}
)

SELECT
    product_id,
    TRIM(product_name) AS product_name,
    TRIM(brand) AS brand,
    TRIM(category) AS category,
    TRIM(subcategory) AS subcategory,
    unit_price,
    cost_price,
    unit_price - cost_price AS gross_margin,
    ROUND((unit_price - cost_price) / NULLIF(unit_price, 0) * 100, 2) AS margin_pct,
    is_prescription,
    is_pharmacy_only,
    is_sister_club_exclusive,
    points_multiplier,
    health_category,
    skin_concern,
    age_group_target,
    gender_target,
    ingredient_highlights,
    is_vegan,
    is_cruelty_free,
    is_organic,
    is_fragrance_free,
    product_size,
    units_per_pack,
    avg_rating,
    review_count,
    launch_date,
    DATEDIFF('day', launch_date, CURRENT_DATE()) AS days_since_launch,
    is_trending,
    is_bestseller,
    complementary_products,
    _ingested_at
FROM source
