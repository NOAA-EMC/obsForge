#!/bin/bash -l

# ==============================================================================
# ObsForge Monitor: Standalone Website Generator
# Use this to regenerate plots/html without re-scanning the filesystem.
# ==============================================================================

# 1. Configuration (Must match run_monitor.sh)
# ------------------------------------------------------------------------------
HOMEobsforge="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/obsForge"
RUN_DIR="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/monitoring"
DATABASE="${RUN_DIR}/emcda.db"
WEB_DIR="${RUN_DIR}/web"
PY_MON="${HOMEobsforge}/utils/monitor"
GEN_SCRIPT="${PY_MON}/generate_site.py"

# 2. Environment Setup
# ------------------------------------------------------------------------------
echo "--- Setting up Environment ---"

# Source Main Setup
if [ -f "${HOMEobsforge}/ush/of_setup.sh" ]; then
    source "${HOMEobsforge}/ush/of_setup.sh"
else
    echo "[FATAL] Setup script not found at ${HOMEobsforge}/ush/of_setup.sh"
    exit 1
fi

# Load Plotting Dependencies
echo "Loading py-matplotlib..."
module load py-matplotlib
if [ $? -ne 0 ]; then
    echo "[FATAL] Failed to load module 'py-matplotlib'. Plotting is impossible."
    exit 1
fi

# 3. Pre-Flight Checks
# ------------------------------------------------------------------------------
echo "--- Checking Prerequisites ---"

if [ ! -f "$DATABASE" ]; then
    echo "[FATAL] Database not found at: $DATABASE"
    echo "Please run 'run_monitor.sh' first to populate the inventory."
    exit 1
fi

if [ ! -f "$GEN_SCRIPT" ]; then
    echo "[FATAL] Python script not found at: $GEN_SCRIPT"
    exit 1
fi

# Ensure output directory exists
mkdir -p "$WEB_DIR"

# 4. Execution
# ------------------------------------------------------------------------------
echo "--- Generating Website ---"
echo "Database: $DATABASE"
echo "Output:   $WEB_DIR"

python3 "$GEN_SCRIPT" \
    --db "$DATABASE" \
    --out "$WEB_DIR"

if [ $? -eq 0 ]; then
    echo "--- SUCCESS ---"
    echo "Website available at: ${WEB_DIR}/index.html"
else
    echo "[FATAL] Python script execution failed."
    exit 1
fi
