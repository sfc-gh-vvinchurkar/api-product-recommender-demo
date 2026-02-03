{{ config(materialized='table', schema='ML') }}

WITH content_recs AS (
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
        recommendation_score,
        recommendation_type,
        recommendation_rank
    FROM {{ ref('content_recommendations') }}
),

collab_recs AS (
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
        recommendation_score,
        recommendation_type,
        recommendation_rank
    FROM {{ ref('collaborative_recommendations') }}
),

combined AS (
    SELECT * FROM content_recs
    UNION ALL
    SELECT * FROM collab_recs
),

deduplicated AS (
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
        MAX(recommendation_score) AS recommendation_score,
        LISTAGG(DISTINCT recommendation_type, ' + ') WITHIN GROUP (ORDER BY recommendation_type) AS recommendation_types,
        COUNT(DISTINCT recommendation_type) AS algorithm_count
    FROM combined
    GROUP BY 
        member_id, product_id, product_name, brand, category, subcategory,
        unit_price, avg_rating, is_bestseller, is_trending
),

final_scored AS (
    SELECT
        d.*,
        m.generation,
        m.gender,
        m.membership_tier,
        m.skin_type,
        m.price_sensitivity,
        -- Boost score if recommended by multiple algorithms
        CASE WHEN algorithm_count > 1 THEN recommendation_score * 1.5 
             ELSE recommendation_score END AS final_score,
        RANK() OVER (PARTITION BY d.member_id ORDER BY 
            CASE WHEN algorithm_count > 1 THEN recommendation_score * 1.5 
                 ELSE recommendation_score END DESC) AS final_rank
    FROM deduplicated d
    JOIN {{ ref('member_features') }} m ON d.member_id = m.member_id
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
    recommendation_score,
    final_score,
    recommendation_types,
    algorithm_count,
    generation AS member_generation,
    gender AS member_gender,
    membership_tier,
    skin_type AS member_skin_type,
    price_sensitivity,
    final_rank,
    CURRENT_TIMESTAMP() AS generated_at
FROM final_scored
WHERE final_rank <= 10
ORDER BY member_id, final_rank
