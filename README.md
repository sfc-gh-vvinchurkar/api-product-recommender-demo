# Product Recommender PoC

**Division**: Australian Pharmaceutical Industries (API/Priceline)  
**Database**: `WESFARMERS_HEALTH_RECOMMENDER`  
**Status**: ✅ Production Ready

## Overview

AI-powered product recommendation engine for Sister Club loyalty members using:
- **Content-Based Filtering**: Match products to member profiles (skin type, age, preferences)
- **Collaborative Filtering**: "Members like you also bought" approach
- **Hybrid Scoring**: Combines both algorithms for optimal recommendations

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   RAW Schema    │────▶│ STAGING Schema  │────▶│ FEATURES Schema │
│                 │     │                 │     │                 │
│ raw_members     │     │ stg_members     │     │ member_features │
│ raw_products    │     │ stg_products    │     │ product_features│
│ raw_transactions│     │ stg_transactions│     │                 │
│ raw_product_    │     │                 │     │                 │
│   views         │     │                 │     │                 │
│ raw_reviews     │     │                 │     │                 │
│ raw_interactions│     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                        ┌───────────────────────────────┘
                        ▼
              ┌─────────────────┐     ┌─────────────────┐
              │    ML Schema    │────▶│   Intelligence  │
              │                 │     │     Agent       │
              │ content_recs    │     │                 │
              │ collab_recs     │     │ Product         │
              │ final_recs      │     │ Recommender     │
              │ member_affinity │     │                 │
              └─────────────────┘     └─────────────────┘
```

## Data Volume

| Table | Records | Description |
|-------|---------|-------------|
| Members | 15,000 | Detailed member profiles |
| Products | 500 | Rich product catalog |
| Transactions | 100,000 | Purchase history |
| Product Views | 500,000 | Browsing behavior |
| Reviews | 25,000 | Product ratings |
| Interactions | 200,000 | Email, search, clicks |

## Quick Start

```bash
./setup/run_setup.sh
```

## Sample Questions (Cortex Analyst)

- "What categories are most recommended?"
- "Show top recommended brands"
- "How do recommendations vary by membership tier?"
- "What products should we recommend to Gen Z members?"
- "Which algorithm performs better - content or collaborative?"

## Recommendation Algorithms

### Content-Based Filtering
Matches products to member attributes:
- Category affinity (purchase history)
- Brand affinity
- Skin type → Skin concern products
- Health interests → Health category products
- Age group targeting
- Gender targeting
- Price sensitivity matching
- Product quality (ratings, bestseller status)

### Collaborative Filtering
Finds similar members based on:
- Generation
- Gender
- Skin type
- Price sensitivity
- Membership tier
- Health interests

Then recommends products bought by similar members but not yet purchased by the target member.

### Hybrid Scoring
- Combines scores from both algorithms
- Products recommended by BOTH algorithms get 1.5x boost
- Final rank determines top 10 recommendations per member
