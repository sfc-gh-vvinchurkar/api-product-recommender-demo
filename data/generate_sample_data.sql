-- =============================================================================
-- Product Recommender PoC - Sample Data Generation
-- Division: Australian Pharmaceutical Industries (API/Priceline)
-- =============================================================================
-- Generates complex, realistic data for product recommendations:
-- - 15,000 members with detailed profiles
-- - 500 products with rich attributes
-- - 100,000 transactions
-- - 500,000 product views
-- - 25,000 reviews
-- - 200,000 interactions
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE WESFARMERS_HEALTH_RECOMMENDER;
USE WAREHOUSE API_RECOMMENDER_WH;

-- =============================================================================
-- PRODUCTS (500 products with rich attributes)
-- =============================================================================
TRUNCATE TABLE IF EXISTS RAW.RAW_PRODUCTS;

INSERT INTO RAW.RAW_PRODUCTS
WITH product_base AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) as n
    FROM TABLE(GENERATOR(ROWCOUNT => 500))
),
brands AS (
    SELECT column1 as brand, column2 as brand_type, ROW_NUMBER() OVER (ORDER BY 1) as id FROM (VALUES
        ('CeraVe','Skincare'),('La Roche-Posay','Skincare'),('Neutrogena','Skincare'),('The Ordinary','Skincare'),
        ('Olay','Skincare'),('Clinique','Skincare'),('Dermaveen','Skincare'),('QV','Skincare'),
        ('Swisse','Vitamins'),('Blackmores','Vitamins'),('Natures Own','Vitamins'),('Centrum','Vitamins'),
        ('Elevit','Vitamins'),('Ostelin','Vitamins'),('Berocca','Vitamins'),('Menevit','Vitamins'),
        ('Maybelline','Makeup'),('LOreal','Makeup'),('Revlon','Makeup'),('MAC','Makeup'),
        ('NYX','Makeup'),('Rimmel','Makeup'),('Urban Decay','Makeup'),('Clinique','Makeup'),
        ('Panadol','Pain Relief'),('Nurofen','Pain Relief'),('Voltaren','Pain Relief'),('Advil','Pain Relief'),
        ('Telfast','Allergy'),('Zyrtec','Allergy'),('Claratyne','Allergy'),('Polaramine','Allergy'),
        ('Oral-B','Dental'),('Colgate','Dental'),('Sensodyne','Dental'),('Listerine','Dental'),
        ('Dove','Personal Care'),('Nivea','Personal Care'),('Palmers','Personal Care'),('Aveeno','Personal Care')
    )
),
categories AS (
    SELECT column1 as category, column2 as subcategory, column3 as health_cat, column4 as skin_concern, ROW_NUMBER() OVER (ORDER BY 1) as id FROM (VALUES
        ('Skincare','Moisturisers','Skin Health','Dryness'),('Skincare','Cleansers','Skin Health','Oiliness'),
        ('Skincare','Serums','Skin Health','Anti-Aging'),('Skincare','Sunscreen','Skin Health','Sun Protection'),
        ('Skincare','Acne Treatment','Skin Health','Acne'),('Skincare','Eye Care','Skin Health','Dark Circles'),
        ('Vitamins','Multivitamins','General Wellness',NULL),('Vitamins','Omega Supplements','Heart Health',NULL),
        ('Vitamins','Vitamin D','Bone Health',NULL),('Vitamins','B Vitamins','Energy Support',NULL),
        ('Vitamins','Prenatal','Pregnancy Support',NULL),('Vitamins','Joint Health','Joint Health',NULL),
        ('Makeup','Foundation','Beauty',NULL),('Makeup','Mascara','Beauty',NULL),
        ('Makeup','Lipstick','Beauty',NULL),('Makeup','Eyeshadow','Beauty',NULL),
        ('Pain Relief','Analgesics','Pain Management',NULL),('Pain Relief','Anti-inflammatory','Pain Management',NULL),
        ('Pain Relief','Topical','Pain Management',NULL),('Allergy','Antihistamines','Allergy Management',NULL),
        ('Dental Care','Toothpaste','Oral Health',NULL),('Dental Care','Mouthwash','Oral Health',NULL),
        ('Personal Care','Body Wash','Personal Care',NULL),('Personal Care','Body Lotion','Personal Care',NULL),
        ('Hair Care','Shampoo','Hair Health',NULL),('Hair Care','Conditioner','Hair Health',NULL),
        ('Baby Care','Baby Skincare','Baby Health',NULL),('Baby Care','Nappies','Baby Health',NULL),
        ('Cold & Flu','Decongestants','Respiratory Health',NULL),('Cold & Flu','Throat Lozenges','Respiratory Health',NULL)
    )
)
SELECT
    'PROD-' || LPAD(n::VARCHAR, 5, '0') as product_id,
    b.brand || ' ' || c.subcategory || ' ' || 
        CASE MOD(n, 5) WHEN 0 THEN 'Premium' WHEN 1 THEN 'Daily' WHEN 2 THEN 'Intensive' WHEN 3 THEN 'Gentle' ELSE 'Advanced' END as product_name,
    b.brand,
    c.category,
    c.subcategory,
    ROUND(5 + MOD(ABS(HASH(n)), 195) + UNIFORM(0::FLOAT, 0.99::FLOAT, RANDOM()), 2) as unit_price,
    ROUND((5 + MOD(ABS(HASH(n)), 195)) * 0.55, 2) as cost_price,
    c.category = 'Prescription' as is_prescription,
    c.category IN ('Pain Relief', 'Allergy', 'Cold & Flu') as is_pharmacy_only,
    MOD(n, 20) = 0 as is_sister_club_exclusive,
    CASE WHEN c.category IN ('Skincare', 'Makeup') THEN 2.0 WHEN c.category = 'Vitamins' THEN 1.5 ELSE 1.0 END as points_multiplier,
    c.health_cat as health_category,
    c.skin_concern,
    CASE MOD(n, 4) WHEN 0 THEN 'All Ages' WHEN 1 THEN '18-35' WHEN 2 THEN '35-55' ELSE '55+' END as age_group_target,
    CASE WHEN c.category = 'Makeup' THEN 'Female' WHEN MOD(n, 10) = 0 THEN 'Male' ELSE 'All' END as gender_target,
    CASE MOD(n, 6) WHEN 0 THEN 'Hyaluronic Acid' WHEN 1 THEN 'Vitamin C' WHEN 2 THEN 'Retinol' 
         WHEN 3 THEN 'Niacinamide' WHEN 4 THEN 'Salicylic Acid' ELSE 'Ceramides' END as ingredient_highlights,
    MOD(n, 8) = 0 as is_vegan,
    MOD(n, 5) = 0 as is_cruelty_free,
    MOD(n, 12) = 0 as is_organic,
    MOD(n, 7) = 0 as is_fragrance_free,
    CASE MOD(n, 5) WHEN 0 THEN '30ml' WHEN 1 THEN '50ml' WHEN 2 THEN '100ml' WHEN 3 THEN '200ml' ELSE '500ml' END as product_size,
    CASE WHEN c.category = 'Vitamins' THEN 30 + MOD(n, 70) ELSE 1 END as units_per_pack,
    ROUND(3.0 + UNIFORM(0::FLOAT, 2::FLOAT, RANDOM()), 2) as avg_rating,
    50 + MOD(ABS(HASH(n * 2)), 950) as review_count,
    DATEADD('day', -MOD(ABS(HASH(n * 3)), 1825), CURRENT_DATE()) as launch_date,
    MOD(n, 15) = 0 as is_trending,
    MOD(n, 10) = 0 as is_bestseller,
    'PROD-' || LPAD((MOD(n + 5, 500) + 1)::VARCHAR, 5, '0') || ',' || 
    'PROD-' || LPAD((MOD(n + 10, 500) + 1)::VARCHAR, 5, '0') as complementary_products,
    CURRENT_TIMESTAMP()
FROM product_base
CROSS JOIN (SELECT brand, brand_type FROM brands WHERE id = 1 + MOD(ABS(HASH(RANDOM())), 40)) b
CROSS JOIN (SELECT category, subcategory, health_cat, skin_concern FROM categories WHERE id = 1 + MOD(ABS(HASH(RANDOM())), 30)) c;

SELECT 'Products loaded: ' || COUNT(*) FROM RAW.RAW_PRODUCTS;

-- =============================================================================
-- MEMBERS (15,000 with detailed profiles)
-- =============================================================================
TRUNCATE TABLE IF EXISTS RAW.RAW_MEMBERS;

INSERT INTO RAW.RAW_MEMBERS
WITH numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) as n
    FROM TABLE(GENERATOR(ROWCOUNT => 15000))
),
first_names AS (
    SELECT column1 as name, ROW_NUMBER() OVER (ORDER BY 1) as id FROM (VALUES
        ('Emma'),('Olivia'),('Charlotte'),('Amelia'),('Isla'),('Mia'),('Ava'),('Grace'),
        ('Sophie'),('Chloe'),('Lily'),('Ruby'),('Oliver'),('Jack'),('Noah'),('William'),
        ('James'),('Lucas'),('Henry'),('Ethan'),('Aisha'),('Mei'),('Priya'),('Fatima')
    )
),
last_names AS (
    SELECT column1 as name, ROW_NUMBER() OVER (ORDER BY 1) as id FROM (VALUES
        ('Smith'),('Jones'),('Williams'),('Brown'),('Wilson'),('Taylor'),('Johnson'),('White'),
        ('Nguyen'),('Chen'),('Patel'),('Singh'),('Lee'),('Kim'),('Wang'),('Garcia')
    )
),
locations AS (
    SELECT column1 as state, column2 as suburb, column3 as postcode, ROW_NUMBER() OVER (ORDER BY 1) as id FROM (VALUES
        ('NSW','Sydney','2000'),('NSW','Parramatta','2150'),('NSW','Bondi','2026'),('NSW','Chatswood','2067'),
        ('VIC','Melbourne','3000'),('VIC','St Kilda','3182'),('VIC','Richmond','3121'),('VIC','Geelong','3220'),
        ('QLD','Brisbane','4000'),('QLD','Gold Coast','4217'),('QLD','Sunshine Coast','4556'),
        ('SA','Adelaide','5000'),('WA','Perth','6000'),('TAS','Hobart','7000'),('ACT','Canberra','2600'),('NT','Darwin','0800')
    )
)
SELECT
    'MEM-' || LPAD(n::VARCHAR, 7, '0') as member_id,
    fn.name as first_name,
    ln.name as last_name,
    LOWER(fn.name) || '.' || LOWER(ln.name) || n || '@' || 
        CASE MOD(n, 5) WHEN 0 THEN 'gmail.com' WHEN 1 THEN 'outlook.com' WHEN 2 THEN 'yahoo.com.au' 
             WHEN 3 THEN 'icloud.com' ELSE 'hotmail.com' END as email,
    '04' || LPAD(ABS(HASH(n))::VARCHAR, 8, '0') as phone,
    DATEADD('day', -6570 - MOD(ABS(HASH(n * 2)), 18250), CURRENT_DATE()) as date_of_birth,
    CASE WHEN MOD(n, 10) < 7 THEN 'Female' ELSE 'Male' END as gender,
    loc.suburb,
    loc.state,
    loc.postcode,
    DATEADD('day', -30 - MOD(ABS(HASH(n * 3)), 1795), CURRENT_DATE()) as join_date,
    CASE WHEN MOD(n, 10) < 4 THEN 'Bronze' WHEN MOD(n, 10) < 7 THEN 'Silver' 
         WHEN MOD(n, 10) < 9 THEN 'Gold' ELSE 'Platinum' END as membership_tier,
    MOD(ABS(HASH(n * 4)), 75000) as total_points,
    ROUND(100 + MOD(ABS(HASH(n * 5)), 29900) + UNIFORM(0::FLOAT, 0.99::FLOAT, RANDOM()), 2) as lifetime_spend,
    'Priceline ' || loc.suburb as preferred_store,
    CASE WHEN MOD(n, 3) = 0 THEN 'Online' ELSE 'In-Store' END as preferred_channel,
    CASE MOD(n, 8) WHEN 0 THEN 'Skincare' WHEN 1 THEN 'Vitamins' WHEN 2 THEN 'Weight Management' 
         WHEN 3 THEN 'Sleep Health' WHEN 4 THEN 'Hair Health' WHEN 5 THEN 'Anti-Aging'
         WHEN 6 THEN 'Pregnancy' ELSE 'General Wellness' END as health_interests,
    CASE MOD(n, 5) WHEN 0 THEN 'Oily' WHEN 1 THEN 'Dry' WHEN 2 THEN 'Combination' 
         WHEN 3 THEN 'Sensitive' ELSE 'Normal' END as skin_type,
    CASE MOD(n, 4) WHEN 0 THEN 'Oily' WHEN 1 THEN 'Dry' WHEN 2 THEN 'Normal' ELSE 'Color-treated' END as hair_type,
    CASE WHEN MOD(n, 15) = 0 THEN 'Fragrance' WHEN MOD(n, 20) = 0 THEN 'Gluten' 
         WHEN MOD(n, 25) = 0 THEN 'Nuts' ELSE NULL END as allergies,
    MOD(n, 4) = 0 as script_customer,
    CASE WHEN MOD(n, 50) = 0 AND MOD(n, 10) < 7 THEN 
        CASE MOD(n, 3) WHEN 0 THEN 'First Trimester' WHEN 1 THEN 'Second Trimester' ELSE 'Third Trimester' END
    ELSE NULL END as pregnancy_stage,
    MOD(n, 4) = 0 as has_children,
    CASE WHEN MOD(n, 4) = 0 THEN 
        CASE MOD(n, 3) WHEN 0 THEN '0-2' WHEN 1 THEN '3-5' ELSE '6-12' END
    ELSE NULL END as children_ages,
    CASE MOD(n, 6) WHEN 0 THEN 'CeraVe,La Roche-Posay' WHEN 1 THEN 'Swisse,Blackmores' 
         WHEN 2 THEN 'Maybelline,LOreal' WHEN 3 THEN 'The Ordinary,Clinique'
         WHEN 4 THEN 'Panadol,Nurofen' ELSE 'Dove,Nivea' END as brand_affinity,
    CASE MOD(n, 3) WHEN 0 THEN 'Budget' WHEN 1 THEN 'Value' ELSE 'Premium' END as price_sensitivity,
    CASE WHEN MOD(n, 20) = 0 THEN 'Inactive' ELSE 'Active' END as account_status,
    CURRENT_TIMESTAMP()
FROM numbers
CROSS JOIN (SELECT name FROM first_names WHERE id = 1 + MOD(ABS(HASH(RANDOM())), 24)) fn
CROSS JOIN (SELECT name FROM last_names WHERE id = 1 + MOD(ABS(HASH(RANDOM())), 16)) ln
CROSS JOIN (SELECT state, suburb, postcode FROM locations WHERE id = 1 + MOD(ABS(HASH(RANDOM())), 16)) loc
WHERE n <= 15000;

SELECT 'Members loaded: ' || COUNT(*) FROM RAW.RAW_MEMBERS;

-- =============================================================================
-- TRANSACTIONS (100,000)
-- =============================================================================
TRUNCATE TABLE IF EXISTS RAW.RAW_TRANSACTIONS;

INSERT INTO RAW.RAW_TRANSACTIONS
WITH numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) as n
    FROM TABLE(GENERATOR(ROWCOUNT => 100000))
),
stores AS (
    SELECT column1 as store_id, column2 as store_name, ROW_NUMBER() OVER (ORDER BY 1) as id FROM (VALUES
        ('S001','Priceline Sydney CBD'),('S002','Priceline Melbourne Central'),('S003','Priceline Brisbane City'),
        ('S004','Priceline Perth CBD'),('S005','Priceline Adelaide Rundle'),('S006','Priceline Parramatta'),
        ('S007','Priceline Chadstone'),('S008','Priceline Bondi Junction'),('S009','Priceline Chatswood'),
        ('S010','Priceline Gold Coast'),('S011','Priceline Sunshine Coast'),('S012','Priceline Geelong'),
        ('S013','Priceline Hobart'),('S014','Priceline Canberra'),('S015','Priceline Darwin')
    )
)
SELECT
    'TXN-' || LPAD(n::VARCHAR, 8, '0') as transaction_id,
    'MEM-' || LPAD((1 + MOD(ABS(HASH(n)), 15000))::VARCHAR, 7, '0') as member_id,
    'PROD-' || LPAD((1 + MOD(ABS(HASH(n * 2)), 500))::VARCHAR, 5, '0') as product_id,
    s.store_id,
    s.store_name,
    DATEADD('day', -MOD(ABS(HASH(n * 3)), 730), CURRENT_DATE()) as transaction_date,
    TIMEADD('minute', 480 + MOD(ABS(HASH(n * 4)), 600), '00:00:00'::TIME) as transaction_time,
    1 + MOD(ABS(HASH(n * 5)), 5) as quantity,
    ROUND(5 + MOD(ABS(HASH(n * 6)), 195) + UNIFORM(0::FLOAT, 0.99::FLOAT, RANDOM()), 2) as unit_price,
    CASE WHEN MOD(n, 4) = 0 THEN ROUND(UNIFORM(1::FLOAT, 25::FLOAT, RANDOM()), 2) ELSE 0 END as discount_amount,
    ROUND((1 + MOD(ABS(HASH(n * 5)), 5)) * (5 + MOD(ABS(HASH(n * 6)), 195)) * UNIFORM(0.8::FLOAT, 1::FLOAT, RANDOM()), 2) as total_amount,
    10 + MOD(ABS(HASH(n * 7)), 190) as points_earned,
    CASE WHEN MOD(n, 8) = 0 THEN 50 + MOD(ABS(HASH(n * 8)), 450) ELSE 0 END as points_redeemed,
    CASE MOD(n, 4) WHEN 0 THEN 'Credit Card' WHEN 1 THEN 'Debit Card' WHEN 2 THEN 'EFTPOS' ELSE 'Cash' END as payment_method,
    CASE WHEN MOD(n, 4) = 0 THEN 'Online' ELSE 'In-Store' END as channel,
    'BASKET-' || LPAD((n / 4)::VARCHAR, 8, '0') as basket_id,
    CURRENT_TIMESTAMP()
FROM numbers
CROSS JOIN (SELECT store_id, store_name FROM stores WHERE id = 1 + MOD(ABS(HASH(RANDOM())), 15)) s
WHERE n <= 100000;

SELECT 'Transactions loaded: ' || COUNT(*) FROM RAW.RAW_TRANSACTIONS;

-- =============================================================================
-- PRODUCT VIEWS (500,000 browsing events)
-- =============================================================================
TRUNCATE TABLE IF EXISTS RAW.RAW_PRODUCT_VIEWS;

INSERT INTO RAW.RAW_PRODUCT_VIEWS
WITH numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) as n
    FROM TABLE(GENERATOR(ROWCOUNT => 500000))
)
SELECT
    'VIEW-' || LPAD(n::VARCHAR, 9, '0') as view_id,
    'MEM-' || LPAD((1 + MOD(ABS(HASH(n)), 15000))::VARCHAR, 7, '0') as member_id,
    'PROD-' || LPAD((1 + MOD(ABS(HASH(n * 2)), 500))::VARCHAR, 5, '0') as product_id,
    DATEADD('second', -MOD(ABS(HASH(n * 3)), 63072000), CURRENT_TIMESTAMP()) as view_timestamp,
    CASE WHEN MOD(n, 3) = 0 THEN 'Online' ELSE 'App' END as channel,
    CASE MOD(n, 4) WHEN 0 THEN 'Mobile' WHEN 1 THEN 'Desktop' WHEN 2 THEN 'Tablet' ELSE 'Mobile' END as device_type,
    'SESS-' || LPAD((n / 5)::VARCHAR, 9, '0') as session_id,
    CASE MOD(n, 5) WHEN 0 THEN 'Search' WHEN 1 THEN 'Category' WHEN 2 THEN 'Recommendation' 
         WHEN 3 THEN 'Email' ELSE 'Direct' END as referrer_type,
    5 + MOD(ABS(HASH(n * 4)), 295) as time_on_page_seconds,
    MOD(n, 5) = 0 as added_to_cart,
    MOD(n, 12) = 0 as added_to_wishlist,
    CURRENT_TIMESTAMP()
FROM numbers
WHERE n <= 500000;

SELECT 'Product views loaded: ' || COUNT(*) FROM RAW.RAW_PRODUCT_VIEWS;

-- =============================================================================
-- PRODUCT REVIEWS (25,000)
-- =============================================================================
TRUNCATE TABLE IF EXISTS RAW.RAW_PRODUCT_REVIEWS;

INSERT INTO RAW.RAW_PRODUCT_REVIEWS
WITH numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) as n
    FROM TABLE(GENERATOR(ROWCOUNT => 25000))
)
SELECT
    'REV-' || LPAD(n::VARCHAR, 7, '0') as review_id,
    'MEM-' || LPAD((1 + MOD(ABS(HASH(n)), 15000))::VARCHAR, 7, '0') as member_id,
    'PROD-' || LPAD((1 + MOD(ABS(HASH(n * 2)), 500))::VARCHAR, 5, '0') as product_id,
    ROUND(1 + UNIFORM(0::FLOAT, 4::FLOAT, RANDOM()), 1) as rating,
    CASE MOD(n, 6) WHEN 0 THEN 'Love it!' WHEN 1 THEN 'Great product' WHEN 2 THEN 'Works well'
         WHEN 3 THEN 'Decent value' WHEN 4 THEN 'Not for me' ELSE 'Amazing results' END as review_title,
    CASE MOD(n, 4) WHEN 0 THEN 'This product has been a game changer for my skincare routine. Highly recommend!'
         WHEN 1 THEN 'Good quality for the price. Would buy again.'
         WHEN 2 THEN 'Takes a while to see results but overall satisfied.'
         ELSE 'Works as described. Fast delivery from Priceline.' END as review_text,
    DATEADD('day', -MOD(ABS(HASH(n * 3)), 365), CURRENT_DATE()) as review_date,
    MOD(n, 3) != 0 as verified_purchase,
    MOD(ABS(HASH(n * 4)), 50) as helpful_votes,
    CASE MOD(n, 5) WHEN 0 THEN 'Oily' WHEN 1 THEN 'Dry' WHEN 2 THEN 'Combination' 
         WHEN 3 THEN 'Sensitive' ELSE 'Normal' END as skin_type_reviewer,
    CASE MOD(n, 5) WHEN 0 THEN '18-24' WHEN 1 THEN '25-34' WHEN 2 THEN '35-44' 
         WHEN 3 THEN '45-54' ELSE '55+' END as age_range_reviewer,
    MOD(n, 5) != 0 as would_recommend,
    CURRENT_TIMESTAMP()
FROM numbers
WHERE n <= 25000;

SELECT 'Reviews loaded: ' || COUNT(*) FROM RAW.RAW_PRODUCT_REVIEWS;

-- =============================================================================
-- MEMBER INTERACTIONS (200,000)
-- =============================================================================
TRUNCATE TABLE IF EXISTS RAW.RAW_MEMBER_INTERACTIONS;

INSERT INTO RAW.RAW_MEMBER_INTERACTIONS
WITH numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) as n
    FROM TABLE(GENERATOR(ROWCOUNT => 200000))
)
SELECT
    'INT-' || LPAD(n::VARCHAR, 8, '0') as interaction_id,
    'MEM-' || LPAD((1 + MOD(ABS(HASH(n)), 15000))::VARCHAR, 7, '0') as member_id,
    CASE MOD(n, 6) WHEN 0 THEN 'email_open' WHEN 1 THEN 'email_click' WHEN 2 THEN 'push_notification'
         WHEN 3 THEN 'search' WHEN 4 THEN 'category_browse' ELSE 'product_click' END as interaction_type,
    DATEADD('second', -MOD(ABS(HASH(n * 2)), 31536000), CURRENT_TIMESTAMP()) as interaction_timestamp,
    CASE WHEN MOD(n, 3) != 0 THEN 'PROD-' || LPAD((1 + MOD(ABS(HASH(n * 3)), 500))::VARCHAR, 5, '0') ELSE NULL END as product_id,
    CASE MOD(n, 8) WHEN 0 THEN 'Skincare' WHEN 1 THEN 'Vitamins' WHEN 2 THEN 'Makeup' 
         WHEN 3 THEN 'Pain Relief' WHEN 4 THEN 'Hair Care' WHEN 5 THEN 'Dental Care'
         WHEN 6 THEN 'Baby Care' ELSE 'Personal Care' END as category,
    CASE WHEN MOD(n, 6) = 3 THEN 
        CASE MOD(n, 5) WHEN 0 THEN 'vitamin c serum' WHEN 1 THEN 'moisturiser dry skin' 
             WHEN 2 THEN 'pain relief' WHEN 3 THEN 'pregnancy vitamins' ELSE 'sunscreen' END
    ELSE NULL END as search_query,
    CASE WHEN MOD(n, 3) = 0 THEN 'Email' WHEN MOD(n, 3) = 1 THEN 'App' ELSE 'Web' END as channel,
    CASE WHEN MOD(n, 6) IN (0, 1) THEN 'CAMP-' || LPAD(MOD(n, 50)::VARCHAR, 4, '0') ELSE NULL END as campaign_id,
    CASE WHEN MOD(n, 6) IN (0, 1) THEN 'EMAIL-' || LPAD(MOD(n, 100)::VARCHAR, 5, '0') ELSE NULL END as email_id,
    MOD(n, 4) = 0 as clicked,
    MOD(n, 12) = 0 as converted,
    CURRENT_TIMESTAMP()
FROM numbers
WHERE n <= 200000;

SELECT 'Interactions loaded: ' || COUNT(*) FROM RAW.RAW_MEMBER_INTERACTIONS;

-- =============================================================================
-- SUMMARY
-- =============================================================================
SELECT 'PRODUCTS' as table_name, COUNT(*) as row_count FROM RAW.RAW_PRODUCTS
UNION ALL SELECT 'MEMBERS', COUNT(*) FROM RAW.RAW_MEMBERS
UNION ALL SELECT 'TRANSACTIONS', COUNT(*) FROM RAW.RAW_TRANSACTIONS
UNION ALL SELECT 'PRODUCT_VIEWS', COUNT(*) FROM RAW.RAW_PRODUCT_VIEWS
UNION ALL SELECT 'PRODUCT_REVIEWS', COUNT(*) FROM RAW.RAW_PRODUCT_REVIEWS
UNION ALL SELECT 'MEMBER_INTERACTIONS', COUNT(*) FROM RAW.RAW_MEMBER_INTERACTIONS;
