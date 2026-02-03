{{ config(materialized='table', schema='ML') }}

WITH member_profile AS (
    SELECT
        mf.member_id,
        mf.generation,
        mf.gender,
        mf.skin_type,
        mf.hair_type,
        mf.health_interests,
        mf.price_sensitivity,
        mf.has_children,
        mf.pregnancy_stage,
        mf.top_category_1,
        mf.top_category_2,
        mf.top_brand_1,
        mf.top_brand_2,
        mf.avg_basket_value
    FROM {{ ref('member_features') }} mf
),

member_purchased_products AS (
    SELECT DISTINCT member_id, product_id
    FROM {{ ref('stg_transactions') }}
),

content_match AS (
    SELECT
        mp.member_id,
        pf.product_id,
        pf.product_name,
        pf.brand,
        pf.category,
        pf.subcategory,
        pf.unit_price,
        pf.avg_rating,
        pf.is_bestseller,
        pf.is_trending,
        pf.skin_concern,
        pf.health_category,
        pf.age_group_target,
        pf.gender_target,
        -- Content-based matching score
        (
            -- Category match
            CASE WHEN pf.category = mp.top_category_1 THEN 30
                 WHEN pf.category = mp.top_category_2 THEN 20
                 ELSE 0 END +
            -- Brand affinity
            CASE WHEN pf.brand = mp.top_brand_1 THEN 25
                 WHEN pf.brand = mp.top_brand_2 THEN 15
                 ELSE 0 END +
            -- Skin type match
            CASE WHEN pf.skin_concern IS NOT NULL AND mp.skin_type IS NOT NULL 
                 AND pf.skin_concern ILIKE '%' || mp.skin_type || '%' THEN 20
                 ELSE 0 END +
            -- Health interest match
            CASE WHEN pf.health_category IS NOT NULL AND mp.health_interests IS NOT NULL
                 AND pf.health_category ILIKE '%' || SPLIT_PART(mp.health_interests, ' ', 1) || '%' THEN 15
                 ELSE 0 END +
            -- Age group match
            CASE WHEN pf.age_group_target = 'All Ages' THEN 5
                 WHEN mp.generation = 'Gen Z' AND pf.age_group_target = '18-35' THEN 15
                 WHEN mp.generation = 'Millennial' AND pf.age_group_target IN ('18-35', '35-55') THEN 15
                 WHEN mp.generation = 'Gen X' AND pf.age_group_target = '35-55' THEN 15
                 WHEN mp.generation = 'Boomer' AND pf.age_group_target = '55+' THEN 15
                 ELSE 0 END +
            -- Gender match
            CASE WHEN pf.gender_target = 'All' THEN 5
                 WHEN pf.gender_target = mp.gender THEN 15
                 ELSE 0 END +
            -- Price sensitivity match
            CASE WHEN mp.price_sensitivity = 'Budget' AND pf.unit_price < 20 THEN 15
                 WHEN mp.price_sensitivity = 'Value' AND pf.unit_price BETWEEN 20 AND 50 THEN 15
                 WHEN mp.price_sensitivity = 'Premium' AND pf.unit_price > 50 THEN 15
                 ELSE 0 END +
            -- Product quality bonus
            CASE WHEN pf.avg_rating >= 4.5 THEN 20
                 WHEN pf.avg_rating >= 4.0 THEN 10
                 ELSE 0 END +
            -- Trending/Bestseller bonus
            CASE WHEN pf.is_bestseller THEN 15 ELSE 0 END +
            CASE WHEN pf.is_trending THEN 10 ELSE 0 END
        ) AS content_score
    FROM member_profile mp
    CROSS JOIN {{ ref('product_features') }} pf
    WHERE NOT EXISTS (
        SELECT 1 FROM member_purchased_products mpp 
        WHERE mpp.member_id = mp.member_id AND mpp.product_id = pf.product_id
    )
)

SELECT
    member_id,
    product_id,
    product_name,
    brand,
    category,
    subcategory,
    unit_price,
    avg_rating,
    is_bestseller,
    is_trending,
    skin_concern,
    health_category,
    content_score AS recommendation_score,
    'Content-Based Filtering' AS recommendation_type,
    RANK() OVER (PARTITION BY member_id ORDER BY content_score DESC) AS recommendation_rank
FROM content_match
WHERE content_score >= 40
QUALIFY recommendation_rank <= 20
