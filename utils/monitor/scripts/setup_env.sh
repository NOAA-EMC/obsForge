#!/bin/bash
# ------------------------------------------------------------------
# Script: scripts/setup_env.sh
# Purpose: Sets up WCOSS2 modules & Python environment (virtual env)
# ------------------------------------------------------------------

# --- 1. Auto-detect project root ---
if [ -z "${PROJECT_ROOT:-}" ]; then
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
    PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." &> /dev/null && pwd )"
    export PROJECT_ROOT
fi

if [ ! -d "${PROJECT_ROOT:-}" ]; then
    echo "CRITICAL ERROR: PROJECT_ROOT is invalid: '${PROJECT_ROOT:-}'"
    return 1 2>/dev/null || exit 1
fi

# --- 2. Set PYTHONPATH ---
export PYTHONPATH="${PROJECT_ROOT}${PYTHONPATH:+:$PYTHONPATH}"
export PYTHONDONTWRITEBYTECODE=1

# --- 3. Detect machine ---
DETECT_SCRIPT="${PROJECT_ROOT}/scripts/detect_machine.sh"
if [ -f "$DETECT_SCRIPT" ]; then
    source "$DETECT_SCRIPT"
else
    MACHINE_ID="wcoss2"
fi

# --- 4. Machine-specific modules ---
case "${MACHINE_ID:-}" in
    "wcoss2")
        module purge >/dev/null 2>&1
        module reset >/dev/null 2>&1
        module load intel/19.1.3.304 >/dev/null 2>&1
        module load ve/evs/2.0 >/dev/null 2>&1

        if ! command -v python3 &> /dev/null; then
            echo "[CRITICAL] Python3 not found after module load"
            return 1 2>/dev/null || exit 1
        fi
        ;;
    *)
        echo "[INFO] Generic/unknown machine. Assuming local Python."
        ;;
esac

VENV_DIR="${HOME}/venvs/db_env"

if [ ! -d "$VENV_DIR" ]; then
    echo "[INFO] Python prerequisites missing or venv not found."

    read -r -p "Create virtual environment at $VENV_DIR and install prerequisites? [Y/n] " answer
    answer=${answer:-Y}

    if [[ "$answer" =~ ^[Yy]$ ]]; then
        "$PROJECT_ROOT/scripts/install_dependencies.sh" "$VENV_DIR" || {
            echo "[ERROR] Failed to install Python dependencies"
            return 1 2>/dev/null || exit 1
        }
    else
        echo "[WARNING] venv not created. Python dependencies may be missing!"
    fi
fi

# --- Activate virtual environment ---
if [ -d "$VENV_DIR" ]; then
    source "${VENV_DIR}/bin/activate"
    echo "[INFO] Virtual environment activated: $VENV_DIR"

    # Ensure the venv python comes first in PATH
    export PATH="${VENV_DIR}/bin:$PATH"
else
    echo "[INFO] Virtual environment not found at $VENV_DIR"
    echo "       Please run install_dependencies.sh first"
    # echo "[INFO] No virtual environment to activate, relying on system Python"
fi

# --- Check Python ---
which python
python --version
python -m pip list | grep SQLAlchemy

