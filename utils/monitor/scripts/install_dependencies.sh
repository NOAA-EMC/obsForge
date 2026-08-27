#!/bin/bash
# ------------------------------------------------------------------
# Script: install_dependencies.sh
# Purpose: Create virtual environment and install Python prerequisites.
# Usage: ./install_dependencies.sh /path/to/venv
# ------------------------------------------------------------------

set -e  # Exit on error

VENV_DIR="$1"

if [ -z "$VENV_DIR" ]; then
    echo "ERROR: No virtual environment path provided."
    echo "Usage: $0 /path/to/venv"
    exit 1
fi

echo "[INFO] Setting up virtual environment at $VENV_DIR"

# 1. Create venv if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR" || {
        echo "[ERROR] Failed to create virtual environment at $VENV_DIR"
        exit 1
    }
    echo "[INFO] Virtual environment created"
else
    echo "[INFO] Virtual environment already exists"
fi

# 2. Activate venv
# shellcheck source=/dev/null
source "$VENV_DIR/bin/activate"

# 3. Upgrade pip safely inside venv (does not affect system pip)
echo "[INFO] Upgrading pip inside virtual environment"
python -m pip install --upgrade pip

# 4. Install required packages
REQUIRED_PACKAGES=(
    "numpy"
    "sqlalchemy"
    "matplotlib"
    "cartopy"
    # Add other packages here as needed
)

echo "[INFO] Installing required Python packages"
pip install --upgrade "${REQUIRED_PACKAGES[@]}"

echo "[INFO] Virtual environment setup complete at $VENV_DIR"
echo "[INFO] Activate it using: source $VENV_DIR/bin/activate"
