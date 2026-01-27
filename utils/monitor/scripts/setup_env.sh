#!/bin/bash
# ------------------------------------------------------------------
# Script: scripts/setup_env.sh
# Purpose: Sets up WCOSS2 System Modules (ve/evs) & Python Path.
# ------------------------------------------------------------------

# 1. Auto-Detect Project Root (If not supplied)
if [ -z "${PROJECT_ROOT}" ]; then
    # Get the directory where THIS script resides
    # ${BASH_SOURCE[0]} works even when the script is sourced
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
    
    # Assume Root is one level up (../)
    PROJECT_ROOT="$( dirname "$SCRIPT_DIR" )"
    
    # Sanity Check: Does the root look right?
    if [ ! -d "${PROJECT_ROOT}/scanner" ]; then
        echo "CRITICAL ERROR: Auto-detected root '${PROJECT_ROOT}' does not contain 'scanner'."
        echo "Please export PROJECT_ROOT manually."
        return 1 2>/dev/null || exit 1
    fi
    # echo "[INFO] Auto-detected PROJECT_ROOT: ${PROJECT_ROOT}"
fi

# 2. Export Python Path
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"
export PYTHONDONTWRITEBYTECODE=1

# 3. Detect Machine
DETECT_SCRIPT="${PROJECT_ROOT}/scripts/detect_machine.sh"
if [ -f "$DETECT_SCRIPT" ]; then
    source "$DETECT_SCRIPT"
else
    # Default fallback
    MACHINE_ID="wcoss2"
fi

# 4. Machine-Specific Loading
if [[ "$MACHINE_ID" == "wcoss2" ]]; then

    module purge >/dev/null 2>&1
    module reset >/dev/null 2>&1
    
    module load intel/19.1.3.304 >/dev/null 2>&1
    module load ve/evs/2.0
    
    # Validation
    if ! command -v python3 &> /dev/null; then
        echo "[CRITICAL] 've/evs/2.0' loaded but python3 is missing."
        return 1 2>/dev/null || exit 1
    fi

elif [[ "$MACHINE_ID" == "ursa" ]]; then
    echo "[INFO] Ursa detected."
else
    echo "[INFO] Generic/Unknown machine. Assuming local python environment."
fi
