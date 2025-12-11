#!/bin/bash -l


## #!/bin/bash
## set +x
## 
## . $MODULESHOME/init/bash        2>/dev/null
## module load core/rocoto/1.3.5   2>/dev/null
## 
## set -x
## 


# ------------------------------------------------------------------
# 1. PRE-FLIGHT SAFETY
# ------------------------------------------------------------------
# Even in a login shell, we set these to prevent crashes in strict scripts.
export PYTHONPATH="${PYTHONPATH:-}"
set +u
set +e

# ------------------------------------------------------------------
# 2. LOAD OBSFORGE
# ------------------------------------------------------------------
# We use the reliable "Find my directory" one-liner you asked about.
# It works perfectly here because we are in a normal shell environment.
HOMEobsforge="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/obsForge"

# We can now source the setup script directly.
# The 'bash -l' environment should have already loaded PrgEnv-intel 
# or set the MODULEPATH so that 'module load' can find it.
if [ -f "${HOMEobsforge}/ush/of_setup.sh" ]; then
    source "${HOMEobsforge}/ush/of_setup.sh"
else
    echo "CRITICAL ERROR: Setup script not found."
    exit 1
fi

# ------------------------------------------------------------------
# 3. RUN MONITOR
# ------------------------------------------------------------------
RUN_DIR="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/monitor_run"
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
