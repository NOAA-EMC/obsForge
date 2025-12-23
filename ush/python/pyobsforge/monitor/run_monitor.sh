#!/bin/bash -l

# 1. Environment Setup
# ------------------------------------------------------------------------------
# Prevent crashes on unbound variables or errors, set python path
export PYTHONPATH="${PYTHONPATH:-}"
set +u
set +e

# Define Root Paths
HOMEobsforge="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/obsForge"

# Source Environment Setup
if [ -f "${HOMEobsforge}/ush/of_setup.sh" ]; then
    source "${HOMEobsforge}/ush/of_setup.sh"
else
    echo "CRITICAL ERROR: Setup script not found at ${HOMEobsforge}/ush/of_setup.sh"
    exit 1
fi

# 2. Configuration
# ------------------------------------------------------------------------------
# Where the database and logs live
RUN_DIR="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/monitoring"
LOG_FILE="${RUN_DIR}/cron_update.log"
DATABASE="${RUN_DIR}/emcda.db"
WEB_DIR="${RUN_DIR}/web"

# Where the raw data lives (The "COMROOT")
DATA_ROOT="/lfs/h2/emc/da/noscrub/emc.da/obsForge/COMROOT/realtime"

# Location of your python scripts
PY_MON="${HOMEobsforge}/ush/python/pyobsforge/monitor"
# ------------------------------------------------------------------------------

# 3. Execution Loop
# ------------------------------------------------------------------------------
{
    echo "=================================================="
    echo "Starting Monitor Pipeline: $(date)"
    echo "Python Executable: $(which python3)"
    
    # Ensure directories exist
    mkdir -p "$WEB_DIR"
    cd "$RUN_DIR" || exit 1

    # --- STEP 1: UPDATE INVENTORY (The Scan) ---
    echo ""
    echo "[STEP 1] Scanning Filesystem..."
    # Scans DATA_ROOT and populates file_inventory table in DATABASE
    python3 "${PY_MON}/update_inventory.py" \
        --db "$DATABASE" \
        --data-root "$DATA_ROOT" \
        --debug

    if [ $? -ne 0 ]; then echo "Error in Step 1 (Scan). Stopping."; exit 1; fi

    # --- STEP 2: LEARN PROPERTIES (The Judge) ---
    echo ""
    echo "[STEP 2] Learning Metadata Truth..."
    # Analyzes history to decide what variables belong in which Obs Space.
    # min-samples=3 means we need 3 agreeing files before locking in the truth.
    python3 "${PY_MON}/learn_schema.py" \
        --db "$DATABASE" \
        --min-samples 3

    # --- STEP 3: VALIDATE INVENTORY (The Enforcer) ---
    echo ""
    echo "[STEP 3] Validating Logical Integrity..."
    # Checks file metadata against the Learned Truth. Flags BAD_META if mismatch.
    python3 "${PY_MON}/validate_inventory.py" \
        --db "$DATABASE"

    # --- STEP 4: GENERATE WEBSITE (The Reporter) ---
    echo ""
    echo "[STEP 4] Generating Static Website..."
    # Uses the Reader to build HTML/CSS report
    python3 "${PY_MON}/generate_site.py" \
        --db "$DATABASE" \
        --out "$WEB_DIR"

    echo "--------------------------------------------------"
    echo "Pipeline Complete: $(date)"
    echo "=================================================="

} >> "$LOG_FILE" 2>&1
