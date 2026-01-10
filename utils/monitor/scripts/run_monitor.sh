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

    echo ""
    echo "[STEP 2] Inspecting Logical Integrity..."
    # Checks file metadata against the Learned Truth.
    python3 "${PY_MON}/inspect_inventory.py" \
        --db "$DATABASE"

    # --- STEP 3: GENERATE WEBSITE (The Reporter) ---
    echo ""
    echo "[STEP 3] Generating Static Website..."
    
    # Attempt to load plotting library.
    # We treat this as optional: if it fails, we skip web generation but don't fail the pipeline.
    if module load py-matplotlib 2>/dev/null; then
        echo "Module 'py-matplotlib' loaded successfully."
        
        python3 "${PY_MON}/generate_site.py" \
            --db "$DATABASE" \
            --out "$WEB_DIR"
            
        if [ $? -ne 0 ]; then
            echo "Error: Website generation script failed."
        else
            echo "Website generated successfully."
        fi
    else
        echo "WARNING: Could not load 'py-matplotlib'. Skipping website generation step."
        echo "To generate the website later, run: ./generate_website.sh"
    fi

    echo "--------------------------------------------------"
    echo "Pipeline Complete: $(date)"
    echo "=================================================="

} >> "$LOG_FILE" 2>&1
