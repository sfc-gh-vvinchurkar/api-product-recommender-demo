{{ config(materialized='table', schema='FEATURES') }}

WITH purchase_history AS (
    SELECT
        t.member_id,
        t.product_id,
        p.category,
        p.subcategory,
        p.brand,
        COUNT(*) AS purchase_count,
        SUM(t.total_amount) AS total_spend,
        AVG(t.total_amount) AS avg_spend,
        MIN(t.transaction_date) AS first_purchase_date,
        MAX(t.transaction_date) AS last_purchase_date,
        DATEDIFF('day', MAX(t.transaction_date), CURRENT_DATE()) AS days_since_last_purchase
    FROM {{ ref('stg_transactions') }} t
    JOIN {{ ref('stg_products') }} p ON t.product_id = p.product_id
    GROUP BY t.member_id, t.product_id, p.category, p.subcategory, p.brand
),

category_affinity AS (
    SELECT
        member_id,
        category,
        SUM(purchase_count) AS category_purchases,
        SUM(total_spend) AS category_spend,
        RANK() OVER (PARTITION BY member_id ORDER BY SUM(total_spend) DESC) AS category_rank
    FROM purchase_history
    GROUP BY member_id, category
),

brand_affinity AS (
    SELECT
        member_id,
        brand,
        SUM(purchase_count) AS brand_purchases,
        SUM(total_spend) AS brand_spend,
        RANK() OVER (PARTITION BY member_id ORDER BY SUM(total_spend) DESC) AS brand_rank
    FROM purchase_history
    GROUP BY member_id, brand
),

member_stats AS (
    SELECT
        member_id,
        COUNT(DISTINCT product_id) AS unique_products_purchased,
        COUNT(DISTINCT category) AS unique_categories,
        COUNT(DISTINCT brand) AS unique_brands,
        SUM(purchase_count) AS total_purchases,
        SUM(total_spend) AS total_spend,
        AVG(avg_spend) AS avg_basket_value,
        MIN(first_purchase_date) AS first_ever_purchase,
        MAX(last_purchase_date) AS last_ever_purchase
    FROM purchase_history
    GROUP BY member_id
)

SELECT
    m.member_id,
    m.generation,
    m.gender,
    m.state,
    m.membership_tier,
    m.skin_type,
    m.hair_type,
    m.health_interests,
    m.price_sensitivity,
    m.preferred_channel,
    m.has_children,
    m.pregnancy_stage,
    ms.unique_products_purchased,
    ms.unique_categories,
    ms.unique_brands,
    ms.total_purchases,
    ms.total_spend,
    ms.avg_basket_value,
    ms.first_ever_purchase,
    ms.last_ever_purchase,
    DATEDIFF('day', ms.last_ever_purchase, CURRENT_DATE()) AS recency_days,
    ca1.category AS top_category_1,
    ca1.category_spend AS top_category_1_spend,
    ca2.category AS top_category_2,
    ca3.category AS top_category_3,
    ba1.brand AS top_brand_1,
    ba1.brand_spend AS top_brand_1_spend,
    ba2.brand AS top_brand_2,
    ba3.brand AS top_brand_3
FROM {{ ref('stg_members') }} m
LEFT JOIN member_stats ms ON m.member_id = ms.member_id
LEFT JOIN category_affinity ca1 ON m.member_id = ca1.member_id AND ca1.category_rank = 1
LEFT JOIN category_affinity ca2 ON m.member_id = ca2.member_id AND ca2.category_rank = 2
LEFT JOIN category_affinity ca3 ON m.member_id = ca3.member_id AND ca3.category_rank = 3
LEFT JOIN brand_affinity ba1 ON m.member_id = ba1.member_id AND ba1.brand_rank = 1
LEFT JOIN brand_affinity ba2 ON m.member_id = ba2.member_id AND ba2.brand_rank = 2
LEFT JOIN brand_affinity ba3 ON m.member_id = ba3.member_id AND ba3.brand_rank = 3
