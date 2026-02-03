{{ config(materialized='table', schema='ML') }}

WITH member_product_interactions AS (
    SELECT
        t.member_id,
        t.product_id,
        'purchase' AS interaction_type,
        COUNT(*) AS interaction_count,
        MAX(t.transaction_date) AS last_interaction_date
    FROM {{ ref('stg_transactions') }} t
    GROUP BY t.member_id, t.product_id
    
    UNION ALL
    
    SELECT
        member_id,
        product_id,
        'view' AS interaction_type,
        COUNT(*) AS interaction_count,
        MAX(view_timestamp::DATE) AS last_interaction_date
    FROM {{ source('raw', 'raw_product_views') }}
    WHERE member_id IS NOT NULL AND product_id IS NOT NULL
    GROUP BY member_id, product_id
    
    UNION ALL
    
    SELECT
        member_id,
        product_id,
        'review' AS interaction_type,
        COUNT(*) AS interaction_count,
        MAX(review_date) AS last_interaction_date
    FROM {{ source('raw', 'raw_product_reviews') }}
    GROUP BY member_id, product_id
),

interaction_scores AS (
    SELECT
        member_id,
        product_id,
        SUM(CASE interaction_type 
            WHEN 'purchase' THEN interaction_count * 10
            WHEN 'view' THEN interaction_count * 1
            WHEN 'review' THEN interaction_count * 5
            ELSE 0 
        END) AS affinity_score,
        MAX(last_interaction_date) AS last_interaction,
        LISTAGG(DISTINCT interaction_type, ',') AS interaction_types
    FROM member_product_interactions
    GROUP BY member_id, product_id
),

member_category_affinity AS (
    SELECT
        m.member_id,
        p.category,
        SUM(CASE 
            WHEN mpi.interaction_type = 'purchase' THEN mpi.interaction_count * 10
            WHEN mpi.interaction_type = 'view' THEN mpi.interaction_count * 1
            ELSE 0 
        END) AS category_affinity
    FROM member_product_interactions mpi
    JOIN {{ ref('stg_products') }} p ON mpi.product_id = p.product_id
    JOIN {{ ref('stg_members') }} m ON mpi.member_id = m.member_id
    GROUP BY m.member_id, p.category
)

SELECT
    i.member_id,
    i.product_id,
    i.affinity_score,
    i.last_interaction,
    i.interaction_types,
    DATEDIFF('day', i.last_interaction, CURRENT_DATE()) AS days_since_interaction,
    CASE 
        WHEN i.affinity_score >= 50 THEN 'High'
        WHEN i.affinity_score >= 20 THEN 'Medium'
        ELSE 'Low'
    END AS affinity_segment,
    mf.generation,
    mf.gender,
    mf.skin_type,
    mf.price_sensitivity,
    mf.membership_tier,
    pf.category,
    pf.subcategory,
    pf.brand,
    pf.avg_rating AS product_rating,
    pf.is_bestseller,
    pf.is_trending
FROM interaction_scores i
JOIN {{ ref('member_features') }} mf ON i.member_id = mf.member_id
JOIN {{ ref('product_features') }} pf ON i.product_id = pf.product_id
WHERE i.affinity_score > 0
