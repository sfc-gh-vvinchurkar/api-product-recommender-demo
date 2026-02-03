{{ config(materialized='view', schema='STAGING') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_members') }}
),

cleaned AS (
    SELECT
        member_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        phone,
        date_of_birth,
        DATEDIFF('year', date_of_birth, CURRENT_DATE()) AS age,
        CASE 
            WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 26 THEN 'Gen Z'
            WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 41 THEN 'Millennial'
            WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 56 THEN 'Gen X'
            ELSE 'Boomer'
        END AS generation,
        gender,
        suburb,
        state,
        postcode,
        join_date,
        DATEDIFF('day', join_date, CURRENT_DATE()) AS days_as_member,
        membership_tier,
        total_points,
        lifetime_spend,
        preferred_store,
        preferred_channel,
        health_interests,
        skin_type,
        hair_type,
        allergies,
        script_customer,
        pregnancy_stage,
        has_children,
        children_ages,
        brand_affinity,
        price_sensitivity,
        account_status,
        _ingested_at
    FROM source
    WHERE account_status = 'Active'
)

SELECT * FROM cleaned
