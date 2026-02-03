-- =============================================================================
-- Product Recommender PoC - Database Setup
-- Division: Australian Pharmaceutical Industries (API/Priceline)
-- =============================================================================
-- Extends Sister Club Loyalty database with recommendation-specific objects
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- Create Database (separate from loyalty demo for isolation)
CREATE DATABASE IF NOT EXISTS WESFARMERS_API_RECOMMENDER
    COMMENT = 'Product Recommender PoC for Australian Pharmaceutical Industries';

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS WESFARMERS_API_RECOMMENDER.RAW 
    COMMENT = 'Raw ingested data';
CREATE SCHEMA IF NOT EXISTS WESFARMERS_API_RECOMMENDER.STAGING 
    COMMENT = 'Cleaned and validated data';
CREATE SCHEMA IF NOT EXISTS WESFARMERS_API_RECOMMENDER.FEATURES 
    COMMENT = 'ML feature engineering';
CREATE SCHEMA IF NOT EXISTS WESFARMERS_API_RECOMMENDER.ML 
    COMMENT = 'ML models and predictions';
CREATE SCHEMA IF NOT EXISTS WESFARMERS_API_RECOMMENDER.ANALYTICS 
    COMMENT = 'Semantic models and dashboards';
CREATE SCHEMA IF NOT EXISTS WESFARMERS_API_RECOMMENDER.GOVERNANCE 
    COMMENT = 'Tags and policies';

-- Create Warehouses
CREATE WAREHOUSE IF NOT EXISTS API_RECOMMENDER_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Product Recommender workloads';

-- Create Stage for Semantic Models
CREATE STAGE IF NOT EXISTS WESFARMERS_API_RECOMMENDER.ANALYTICS.SEMANTIC_MODELS
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Semantic model YAML files';

-- =============================================================================
-- RAW TABLES - Extended for Recommendations
-- =============================================================================

-- Members (core demographics + preferences)
CREATE TABLE IF NOT EXISTS WESFARMERS_API_RECOMMENDER.RAW.RAW_MEMBERS (
    member_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    date_of_birth DATE,
    gender VARCHAR(20),
    suburb VARCHAR(100),
    state VARCHAR(10),
    postcode VARCHAR(10),
    join_date DATE,
    membership_tier VARCHAR(20),
    total_points NUMBER(12,0),
    lifetime_spend NUMBER(12,2),
    preferred_store VARCHAR(100),
    preferred_channel VARCHAR(20),
    health_interests VARCHAR(500),
    skin_type VARCHAR(50),
    hair_type VARCHAR(50),
    allergies VARCHAR(500),
    script_customer BOOLEAN,
    pregnancy_stage VARCHAR(50),
    has_children BOOLEAN,
    children_ages VARCHAR(100),
    brand_affinity VARCHAR(500),
    price_sensitivity VARCHAR(20),
    account_status VARCHAR(20),
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Products (detailed catalog with attributes)
CREATE TABLE IF NOT EXISTS WESFARMERS_API_RECOMMENDER.RAW.RAW_PRODUCTS (
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    brand VARCHAR(100),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    unit_price NUMBER(10,2),
    cost_price NUMBER(10,2),
    is_prescription BOOLEAN,
    is_pharmacy_only BOOLEAN,
    is_sister_club_exclusive BOOLEAN,
    points_multiplier NUMBER(3,1),
    health_category VARCHAR(100),
    skin_concern VARCHAR(100),
    age_group_target VARCHAR(50),
    gender_target VARCHAR(20),
    ingredient_highlights VARCHAR(500),
    is_vegan BOOLEAN,
    is_cruelty_free BOOLEAN,
    is_organic BOOLEAN,
    is_fragrance_free BOOLEAN,
    product_size VARCHAR(50),
    units_per_pack NUMBER(5,0),
    avg_rating NUMBER(3,2),
    review_count NUMBER(8,0),
    launch_date DATE,
    is_trending BOOLEAN,
    is_bestseller BOOLEAN,
    complementary_products VARCHAR(500),
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Transactions (purchase history)
CREATE TABLE IF NOT EXISTS WESFARMERS_API_RECOMMENDER.RAW.RAW_TRANSACTIONS (
    transaction_id VARCHAR(50) NOT NULL,
    member_id VARCHAR(50),
    product_id VARCHAR(50),
    store_id VARCHAR(20),
    store_name VARCHAR(100),
    transaction_date DATE,
    transaction_time TIME,
    quantity NUMBER(6,0),
    unit_price NUMBER(10,2),
    discount_amount NUMBER(10,2),
    total_amount NUMBER(10,2),
    points_earned NUMBER(8,0),
    points_redeemed NUMBER(8,0),
    payment_method VARCHAR(30),
    channel VARCHAR(20),
    basket_id VARCHAR(50),
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Product Views (browsing behavior)
CREATE TABLE IF NOT EXISTS WESFARMERS_API_RECOMMENDER.RAW.RAW_PRODUCT_VIEWS (
    view_id VARCHAR(50) NOT NULL,
    member_id VARCHAR(50),
    product_id VARCHAR(50),
    view_timestamp TIMESTAMP_NTZ,
    channel VARCHAR(20),
    device_type VARCHAR(30),
    session_id VARCHAR(50),
    referrer_type VARCHAR(50),
    time_on_page_seconds NUMBER(6,0),
    added_to_cart BOOLEAN,
    added_to_wishlist BOOLEAN,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Product Ratings & Reviews
CREATE TABLE IF NOT EXISTS WESFARMERS_API_RECOMMENDER.RAW.RAW_PRODUCT_REVIEWS (
    review_id VARCHAR(50) NOT NULL,
    member_id VARCHAR(50),
    product_id VARCHAR(50),
    rating NUMBER(2,1),
    review_title VARCHAR(200),
    review_text VARCHAR(2000),
    review_date DATE,
    verified_purchase BOOLEAN,
    helpful_votes NUMBER(6,0),
    skin_type_reviewer VARCHAR(50),
    age_range_reviewer VARCHAR(30),
    would_recommend BOOLEAN,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Member Interactions (clicks, searches, etc.)
CREATE TABLE IF NOT EXISTS WESFARMERS_API_RECOMMENDER.RAW.RAW_MEMBER_INTERACTIONS (
    interaction_id VARCHAR(50) NOT NULL,
    member_id VARCHAR(50),
    interaction_type VARCHAR(50),
    interaction_timestamp TIMESTAMP_NTZ,
    product_id VARCHAR(50),
    category VARCHAR(100),
    search_query VARCHAR(255),
    channel VARCHAR(20),
    campaign_id VARCHAR(50),
    email_id VARCHAR(50),
    clicked BOOLEAN,
    converted BOOLEAN,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
