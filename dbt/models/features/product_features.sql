{{ config(materialized='table', schema='FEATURES') }}

WITH purchase_stats AS (
    SELECT
        t.product_id,
        COUNT(DISTINCT t.member_id) AS unique_buyers,
        COUNT(*) AS total_purchases,
        SUM(t.total_amount) AS total_revenue,
        AVG(t.total_amount) AS avg_transaction_value,
        SUM(t.quantity) AS total_units_sold,
        AVG(t.quantity) AS avg_units_per_transaction,
        COUNT(DISTINCT t.transaction_date) AS purchase_days,
        MIN(t.transaction_date) AS first_sale_date,
        MAX(t.transaction_date) AS last_sale_date
    FROM {{ ref('stg_transactions') }} t
    GROUP BY t.product_id
),

view_stats AS (
    SELECT
        product_id,
        COUNT(*) AS total_views,
        COUNT(DISTINCT member_id) AS unique_viewers,
        AVG(time_on_page_seconds) AS avg_time_on_page,
        SUM(CASE WHEN added_to_cart THEN 1 ELSE 0 END) AS add_to_cart_count,
        SUM(CASE WHEN added_to_wishlist THEN 1 ELSE 0 END) AS wishlist_count
    FROM {{ source('raw', 'raw_product_views') }}
    GROUP BY product_id
),

review_stats AS (
    SELECT
        product_id,
        COUNT(*) AS review_count,
        AVG(rating) AS avg_rating,
        SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) AS positive_reviews,
        SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) AS negative_reviews,
        SUM(CASE WHEN would_recommend THEN 1 ELSE 0 END) AS would_recommend_count
    FROM {{ source('raw', 'raw_product_reviews') }}
    GROUP BY product_id
),

co_purchase AS (
    SELECT
        t1.product_id,
        COUNT(DISTINCT t2.product_id) AS frequently_bought_together_count
    FROM {{ ref('stg_transactions') }} t1
    JOIN {{ ref('stg_transactions') }} t2 
        ON t1.basket_id = t2.basket_id AND t1.product_id != t2.product_id
    GROUP BY t1.product_id
)

SELECT
    p.product_id,
    p.product_name,
    p.brand,
    p.category,
    p.subcategory,
    p.unit_price,
    p.margin_pct,
    p.health_category,
    p.skin_concern,
    p.age_group_target,
    p.gender_target,
    p.is_vegan,
    p.is_cruelty_free,
    p.is_organic,
    p.is_fragrance_free,
    p.is_trending,
    p.is_bestseller,
    p.days_since_launch,
    COALESCE(ps.unique_buyers, 0) AS unique_buyers,
    COALESCE(ps.total_purchases, 0) AS total_purchases,
    COALESCE(ps.total_revenue, 0) AS total_revenue,
    COALESCE(ps.avg_transaction_value, 0) AS avg_transaction_value,
    COALESCE(ps.total_units_sold, 0) AS total_units_sold,
    COALESCE(vs.total_views, 0) AS total_views,
    COALESCE(vs.unique_viewers, 0) AS unique_viewers,
    COALESCE(vs.avg_time_on_page, 0) AS avg_time_on_page,
    COALESCE(vs.add_to_cart_count, 0) AS add_to_cart_count,
    CASE WHEN vs.total_views > 0 THEN ROUND(vs.add_to_cart_count / vs.total_views * 100, 2) ELSE 0 END AS cart_rate_pct,
    CASE WHEN vs.unique_viewers > 0 THEN ROUND(ps.unique_buyers / vs.unique_viewers * 100, 2) ELSE 0 END AS view_to_purchase_rate,
    COALESCE(rs.review_count, 0) AS review_count,
    COALESCE(rs.avg_rating, 0) AS avg_rating,
    CASE WHEN rs.review_count > 0 THEN ROUND(rs.positive_reviews / rs.review_count * 100, 2) ELSE 0 END AS positive_review_pct,
    CASE WHEN rs.review_count > 0 THEN ROUND(rs.would_recommend_count / rs.review_count * 100, 2) ELSE 0 END AS recommend_rate_pct,
    COALESCE(cp.frequently_bought_together_count, 0) AS co_purchase_product_count,
    p.complementary_products
FROM {{ ref('stg_products') }} p
LEFT JOIN purchase_stats ps ON p.product_id = ps.product_id
LEFT JOIN view_stats vs ON p.product_id = vs.product_id
LEFT JOIN review_stats rs ON p.product_id = rs.product_id
LEFT JOIN co_purchase cp ON p.product_id = cp.product_id
