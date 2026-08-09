#!/bin/bash -l
# ------------------------------------------------------------------
# Script: run_monitor.sh
# Purpose: Main Pipeline Driver.
# ------------------------------------------------------------------

# stop on errors, wrong vars, broken pipelines
set -euo pipefail

# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# echo "SCRIPT DIR: $SCRIPT_DIR"
RUN_DIR="$(pwd)"
# echo "RUN DIR:    $RUN_DIR"

# ==================================================================
# USER CONFIGURATION
# ==================================================================

# DATA_ROOT="/lfs/h2/emc/da/noscrub/emc.da/obsForge/COMROOT/realtime"
# DB_NAME="emcda"

# DATA_ROOT="/lfs/h2/emc/da/noscrub/Hyundeok.Choi/obsForge_realtime/COMROOT/obsforge/"
# DB_NAME="hyundeok"

DATA_ROOT="/scratch3/NCEPDEV/da/Edward.Givelberg/monitoring/data"
DB_NAME="diags"

# for development I use a copy of obsforge in the run dir
# RESET THIS IF YOUR CODE IS ELSEWHERE
export PROJECT_ROOT="${RUN_DIR}/obsForge/utils/ncdb"

# ==================================================================
# AUTOMATION
# ==================================================================

LOG_FILE="${RUN_DIR}/alchemy.log"
DATABASE="${RUN_DIR}/${DB_NAME}.db"
WEB_DIR="${RUN_DIR}/web"
DATA_PRODUCTS_DIR="${RUN_DIR}/data_products"

SETUP_ENV_SCRIPT="${PROJECT_ROOT}/scripts/setup_env.sh"

# the number of cycles to scan
# Default to 0 (All) if not set in environment
LIMIT_CYCLES=${LIMIT_CYCLES:-0}
LIMIT_CYCLES=8      # OVERWRITING for DEBUGGING/DEVELOPMENT


mkdir -p "$RUN_DIR"
mkdir -p "$WEB_DIR"
mkdir -p "$DATA_PRODUCTS_DIR"

# 1. Activate Environment
if [ -f "$SETUP_ENV_SCRIPT" ]; then
    source "$SETUP_ENV_SCRIPT"
else
    echo "CRITICAL ERROR: Environment setup script not found: $SETUP_ENV_SCRIPT"
    exit 1
fi


# 2. Execution Loop
{
    echo "=================================================="
    echo "Starting Monitor Pipeline: $(date)"
    echo "Run Dir: $RUN_DIR"
    echo "Database: $DATABASE"
    echo "Cycle Limit: $LIMIT_CYCLES"
    echo "=================================================="

    cd "$RUN_DIR" || exit 1

    echo ""
    python3 "${PROJECT_ROOT}/app/driver.py" \
        --db "$DATABASE" \
        --data-root "$DATA_ROOT" \
        --limit-cycles "$LIMIT_CYCLES" \
        --data-products-root "$DATA_PRODUCTS_DIR" \
        --web-dir "$WEB_DIR"
        # ### --debug

    echo "Pipeline Complete: $(date)"

} >> "$LOG_FILE" 2>&1
