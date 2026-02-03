#!/bin/bash
# =============================================================================
# Product Recommender PoC - Complete Setup Script
# Division: Australian Pharmaceutical Industries (API/Priceline)
# =============================================================================

set -e

CONN="${SNOWFLAKE_CONNECTION:-sedemo}"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "=============================================="
echo "Product Recommender PoC Setup"
echo "Connection: $CONN"
echo "=============================================="

echo ""
echo "[1/4] Creating database and schemas..."
snow sql -f "$PROJECT_DIR/setup/01_database_setup.sql" -c "$CONN"

echo ""
echo "[2/4] Loading sample data (15k members, 500 products, 100k transactions, 500k views)..."
snow sql -f "$PROJECT_DIR/data/generate_sample_data.sql" -c "$CONN"

echo ""
echo "[3/4] Running dbt transformations (features + recommendations)..."
cd "$PROJECT_DIR/dbt"
snow dbt execute -c "$CONN" --database WESFARMERS_API_RECOMMENDER --schema STAGING api_product_recommender run

echo ""
echo "[4/4] Deploying semantic model..."
snow stage copy "$PROJECT_DIR/semantic_models/product_recommender.yaml" \
    @WESFARMERS_API_RECOMMENDER.ANALYTICS.SEMANTIC_MODELS/ -c "$CONN" --overwrite

echo ""
echo "=============================================="
echo "Setup Complete!"
echo "=============================================="
echo ""
echo "Sample queries for Snowflake Intelligence:"
echo "  - What categories are most recommended?"
echo "  - Show top recommended brands"
echo "  - How do recommendations vary by membership tier?"
echo "  - Which algorithm performs better?"
echo ""
