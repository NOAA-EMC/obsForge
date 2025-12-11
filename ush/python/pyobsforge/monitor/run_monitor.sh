#!/bin/bash

# ------------------------------------------------------------------
# HARDCODED PATHS
# ------------------------------------------------------------------
# 1. Location of the source code
export HOMEobsforge="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/obsForge"

# 2. Location of this runtime instance (DB, YAML, Logs)
export RUN_DIR="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/monitor_run"

# 3. Log file for the cron execution
LOG_FILE="${RUN_DIR}/cron_update.log"

# ------------------------------------------------------------------
# EXECUTION
# ------------------------------------------------------------------

# Redirect all stdout and stderr to the log file
{
    echo "=================================================="
    echo "Starting Monitor Update: $(date)"
    echo "=================================================="

    # A. Load Environment
    if [ -f "${HOMEobsforge}/ush/of_setup.sh" ]; then
        source "${HOMEobsforge}/ush/of_setup.sh"
    else
        echo "CRITICAL ERROR: Setup script not found at ${HOMEobsforge}/ush/of_setup.sh"
        exit 1
    fi

    # B. Set PYTHONPATH
    # Ensure the python scripts in the repo are importable
    export PYTHONPATH="${HOMEobsforge}/ush/python:${PYTHONPATH}"

    # C. Go to Run Directory
    # This ensures we find monitor_config.yaml in the current folder
    cd "$RUN_DIR" || { echo "CRITICAL ERROR: Could not cd to $RUN_DIR"; exit 1; }

    # D. Run the Python Driver
    # We execute the script from the repo, pointing it to the local config
    PYTHON_SCRIPT="${HOMEobsforge}/ush/python/pyobsforge/monitor/monitor_update.py"
    
    echo "Executing: python3 $PYTHON_SCRIPT -c monitor_config.yaml"
    python3 "$PYTHON_SCRIPT" -c monitor_config.yaml

    EXIT_CODE=$?
    echo "--------------------------------------------------"
    echo "Finished Monitor Update: $(date)"
    echo "Exit Code: $EXIT_CODE"
    echo "=================================================="

} >> "$LOG_FILE" 2>&1
