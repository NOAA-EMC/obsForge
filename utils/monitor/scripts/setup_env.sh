#!/bin/bash
# ------------------------------------------------------------------
# Script: scripts/setup_env.sh
# Purpose: Sets up WCOSS2 System Modules (ve/evs) & Python Path.
# ------------------------------------------------------------------

# 1. Auto-Detect Project Root
# This logic assumes the script is located in a subdirectory (like /scripts)
# and the Project Root is one level up.
if [ -z "${PROJECT_ROOT:-}" ]; then
    # Get the directory where THIS script resides
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
    
    # Jump up one level to the parent directory
    PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." &> /dev/null && pwd )"
    export PROJECT_ROOT
fi

# Final check: Ensure PROJECT_ROOT is a valid directory
if [ ! -d "${PROJECT_ROOT:-}" ]; then
    echo "CRITICAL ERROR: PROJECT_ROOT is invalid: '${PROJECT_ROOT:-}'"
    return 1 2>/dev/null || exit 1
fi

# 2. Export Python Path (Safe Expansion)
# ${PYTHONPATH:+:$PYTHONPATH} appends the existing path ONLY if it is already set.
# This prevents "unbound variable" errors and trailing colons.
export PYTHONPATH="${PROJECT_ROOT}${PYTHONPATH:+:$PYTHONPATH}"
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
case "${MACHINE_ID:-}" in
    "wcoss2")
        # Standard WCOSS2 cleanup
        module purge >/dev/null 2>&1
        module reset >/dev/null 2>&1

        # Loading specific compiler and EVS environment
        module load intel/19.1.3.304 >/dev/null 2>&1
        module load ve/evs/2.0 >/dev/null 2>&1

        # Validation: Ensure the module actually did its job
        if ! command -v python3 &> /dev/null; then
            echo "[CRITICAL] 've/evs/2.0' loaded but python3 is missing from PATH."
            return 1 2>/dev/null || exit 1
        fi
        ;;

    "ursa")
        echo "[INFO] Ursa detected."
        # Add Ursa-specific module loads here
        ;;

    *)
        echo "[INFO] Generic/Unknown machine. Assuming local python environment."
        ;;
esac
