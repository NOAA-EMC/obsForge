#!/bin/bash -l
# ------------------------------------------------------------------
# Script: run_monitor.sh
# Purpose: Main Pipeline Driver.
# ------------------------------------------------------------------

# ==================================================================
# USER CONFIGURATION
# ==================================================================

# export PROJECT_ROOT="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/obsForge/utils/monitor"
# RUN_DIR="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/emcda_monitoring"
# DATA_ROOT="/lfs/h2/emc/da/noscrub/emc.da/obsForge/COMROOT/realtime"

DATA_ROOT="/lfs/h2/emc/da/noscrub/Hyundeok.Choi/obsForge_realtime/COMROOT/obsforge/"
RUN_DIR="/u/edward.givelberg/ns/hyundeok_monitoring"
# for development I use a copy of obsforge in the run dir
export PROJECT_ROOT="${RUN_DIR}/obsForge/utils/monitor"

# ==================================================================
# AUTOMATION
# ==================================================================

LOG_FILE="${RUN_DIR}/cron_update.log"
DATABASE="${RUN_DIR}/hyundeok.db"
WEB_DIR="${RUN_DIR}/web"

SETUP_ENV_SCRIPT="${PROJECT_ROOT}/scripts/setup_env.sh"

# the number of cycles to scan
# Default to 0 (All) if not set in environment
LIMIT_CYCLES=${LIMIT_CYCLES:-0}
LIMIT_CYCLES=2      # OVERWRITING for DEBUGGING/DEVELOPMENT


mkdir -p "$RUN_DIR"
mkdir -p "$WEB_DIR"

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
    echo "Cycle Limit: $LIMIT_CYCLES"
    echo "=================================================="

    cd "$RUN_DIR" || exit 1

    # --- STEP 1: SCAN ---
    echo ""
    echo "[STEP 1] Scanning Filesystem..."
    python3 "${PROJECT_ROOT}/update_inventory.py" \
        --db "$DATABASE" \
        --data-root "$DATA_ROOT" \
        --limit-cycles "$LIMIT_CYCLES" \
        --debug

    if [ $? -ne 0 ]; then 
        echo "Error in Step 1 (Scan). Stopping."
        exit 1
    fi

    # --- STEP 2: INSPECT ---
    echo ""
    echo "[STEP 2] Inspecting..."
    python3 "${PROJECT_ROOT}/inspect_inventory.py" \
        --db "$DATABASE"

    # --- STEP 3: REPORT ---
    echo ""
    echo "[STEP 3] Generating Website..."
    
    python3 "${PROJECT_ROOT}/generate_site.py" \
        --data-root "$DATA_ROOT" \
        --db "$DATABASE" \
        --out "$WEB_DIR"

    if [ $? -ne 0 ]; then
        echo "Error: Website generation failed."
    else
        echo "Website generated successfully."
    fi

    echo "Pipeline Complete: $(date)"

} >> "$LOG_FILE" 2>&1
