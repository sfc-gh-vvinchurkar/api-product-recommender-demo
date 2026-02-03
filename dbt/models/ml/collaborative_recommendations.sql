{{ config(materialized='table', schema='ML') }}

WITH similar_members AS (
    SELECT
        m1.member_id AS target_member_id,
        m2.member_id AS similar_member_id,
        -- Similarity score based on shared attributes
        (CASE WHEN m1.generation = m2.generation THEN 20 ELSE 0 END +
         CASE WHEN m1.gender = m2.gender THEN 15 ELSE 0 END +
         CASE WHEN m1.skin_type = m2.skin_type THEN 25 ELSE 0 END +
         CASE WHEN m1.price_sensitivity = m2.price_sensitivity THEN 15 ELSE 0 END +
         CASE WHEN m1.membership_tier = m2.membership_tier THEN 10 ELSE 0 END +
         CASE WHEN m1.health_interests = m2.health_interests THEN 15 ELSE 0 END) AS similarity_score
    FROM {{ ref('member_features') }} m1
    JOIN {{ ref('member_features') }} m2 ON m1.member_id != m2.member_id
    WHERE (CASE WHEN m1.generation = m2.generation THEN 20 ELSE 0 END +
           CASE WHEN m1.gender = m2.gender THEN 15 ELSE 0 END +
           CASE WHEN m1.skin_type = m2.skin_type THEN 25 ELSE 0 END +
           CASE WHEN m1.price_sensitivity = m2.price_sensitivity THEN 15 ELSE 0 END +
           CASE WHEN m1.membership_tier = m2.membership_tier THEN 10 ELSE 0 END +
           CASE WHEN m1.health_interests = m2.health_interests THEN 15 ELSE 0 END) >= 50
    QUALIFY ROW_NUMBER() OVER (PARTITION BY m1.member_id ORDER BY similarity_score DESC) <= 100
),

similar_member_purchases AS (
    SELECT
        sm.target_member_id,
        t.product_id,
        COUNT(DISTINCT sm.similar_member_id) AS similar_members_who_bought,
        AVG(sm.similarity_score) AS avg_similarity_score
    FROM similar_members sm
    JOIN {{ ref('stg_transactions') }} t ON sm.similar_member_id = t.member_id
    -- Exclude products already purchased by target member
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ ref('stg_transactions') }} t2 
        WHERE t2.member_id = sm.target_member_id AND t2.product_id = t.product_id
    )
    GROUP BY sm.target_member_id, t.product_id
    HAVING COUNT(DISTINCT sm.similar_member_id) >= 3
),

collaborative_scores AS (
    SELECT
        smp.target_member_id AS member_id,
        smp.product_id,
        smp.similar_members_who_bought,
        smp.avg_similarity_score,
        pf.product_name,
        pf.brand,
        pf.category,
        pf.subcategory,
        pf.unit_price,
        pf.avg_rating,
        pf.is_bestseller,
        pf.is_trending,
        -- Recommendation score
        ROUND(
            (smp.similar_members_who_bought * 5) + 
            (smp.avg_similarity_score * 0.5) +
            (pf.avg_rating * 10) +
            (CASE WHEN pf.is_bestseller THEN 20 ELSE 0 END) +
            (CASE WHEN pf.is_trending THEN 15 ELSE 0 END),
        2) AS recommendation_score
    FROM similar_member_purchases smp
    JOIN {{ ref('product_features') }} pf ON smp.product_id = pf.product_id
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
    similar_members_who_bought,
    avg_similarity_score,
    recommendation_score,
    'Collaborative Filtering' AS recommendation_type,
    RANK() OVER (PARTITION BY member_id ORDER BY recommendation_score DESC) AS recommendation_rank
FROM collaborative_scores
QUALIFY recommendation_rank <= 20
