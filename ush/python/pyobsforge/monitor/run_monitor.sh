#!/bin/bash -l

# Even in a login shell, we set these to prevent crashes in strict scripts.
export PYTHONPATH="${PYTHONPATH:-}"
set +u
set +e

HOMEobsforge="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/obsForge"

if [ -f "${HOMEobsforge}/ush/of_setup.sh" ]; then
    source "${HOMEobsforge}/ush/of_setup.sh"
else
    echo "CRITICAL ERROR: Setup script not found."
    exit 1
fi

RUN_DIR="/lfs/h2/emc/da/noscrub/hyundeok.choi/monitoring"
LOG_FILE="${RUN_DIR}/cron_update.log"

{
    echo "=================================================="
    echo "Starting Monitor Update: $(date)"
    
    # Debug: Show which python we found (should be from the module)
    echo "Python executable: $(which python3)"
    
    cd "$RUN_DIR" || exit 1

    PYTHON_SCRIPT="${HOMEobsforge}/ush/python/pyobsforge/monitor/monitor_update.py"
    
    echo "Executing: python3 $PYTHON_SCRIPT -c monitor_config.yaml"
    python3 "$PYTHON_SCRIPT" -c monitor_config.yaml

    EXIT_CODE=$?
    echo "--------------------------------------------------"
    echo "Finished Monitor Update: $(date)"
    echo "Exit Code: $EXIT_CODE"
    echo "=================================================="

} >> "$LOG_FILE" 2>&1
