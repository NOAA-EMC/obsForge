#!/bin/bash -l

# ==============================================================================
# ObsForge Monitor: Standalone Website Generator
# Use this to regenerate plots/html without re-scanning the filesystem.
# ==============================================================================

# 1. Configuration (Must match run_monitor.sh)
# ------------------------------------------------------------------------------
PROJECT_ROOT="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/obsForge/utils/monitor"

RUN_DIR="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/emcda_monitoring"
DATABASE="${RUN_DIR}/emcda.db"
WEB_DIR="${RUN_DIR}/web"

SETUP_ENV_SCRIPT="${PROJECT_ROOT}/scripts/setup_env.sh"
GEN_SCRIPT="${PROJECT_ROOT}/generate_site.py"


# 2. Environment Setup
# ------------------------------------------------------------------------------
echo "--- Setting up Environment ---"

# Source Main Setup
if [ -f "${SETUP_ENV_SCRIPT}" ]; then
    source "${SETUP_ENV_SCRIPT}"
else
    echo "[FATAL] Setup script not found ${SETUP_ENV_SCRIPT}"
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
