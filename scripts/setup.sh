#!/usr/bin/env bash
# =============================================================================
# setup.sh — One-command setup for the BSC Order Status Assistant demo.
#
# Steps:
#   1. Install Python dependencies (API + infra)
#   2. Generate and load synthetic data into Snowflake
#   3. Print next-steps instructions
#
# The dbt project is deployed via dbt Cloud — not run locally.
# See README.md for dbt Cloud setup instructions.
#
# Prerequisites:
#   - Python 3.11+
#   - .env with valid Snowflake credentials
#
# Usage:
#   ./scripts/setup.sh [--skip-data] [--dry-run]
# =============================================================================

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

SKIP_DATA=false
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        --skip-data) SKIP_DATA=true ;;
        --dry-run)   DRY_RUN=true ;;
    esac
done

echo "============================================"
echo "  BSC Order Status Assistant — Setup"
echo "============================================"
echo ""

# --- Check .env ---
if [ ! -f .env ]; then
    echo "ERROR: .env file not found. Copy .env.example to .env and fill in credentials."
    exit 1
fi

# --- Install API deps ---
echo "[1/3] Installing API dependencies..."
pip install -q -r api/requirements.txt

# --- Install infra deps ---
echo "[2/3] Installing infra/data-gen dependencies..."
pip install -q -r infra/scripts/requirements.txt

# --- Generate + load data ---
if [ "$SKIP_DATA" = false ]; then
    echo "[3/3] Generating and loading synthetic dataset..."
    if [ "$DRY_RUN" = true ]; then
        python infra/scripts/generate_and_load.py --dry-run
    else
        python infra/scripts/generate_and_load.py
    fi
else
    echo "[3/3] Skipping data generation (--skip-data)"
fi

echo ""
echo "============================================"
echo "  Setup complete!"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Start the API:   uvicorn api.main:app --reload"
echo "  2. Start the UI:    cd ui && streamlit run app.py"
echo "  3. Or use Docker:   docker compose up --build"
echo ""
echo "  API docs:   http://localhost:8000/docs"
echo "  Streamlit:  http://localhost:8501"
echo ""
echo "To connect dbt Cloud Semantic Layer:"
echo "  1. Push the dbt/ folder to a Git repo connected to your dbt Cloud project"
echo "  2. Run a production deployment in dbt Cloud"
echo "  3. Add DBT_CLOUD_HOST, DBT_CLOUD_TOKEN, DBT_CLOUD_ENVIRONMENT_ID to .env"
echo "  4. Set SEMANTIC_BACKEND=dbt_mcp in .env"
echo ""
