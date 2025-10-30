#! /bin/bash

#
# Resets the lmod environment and loads the modules necessary to run all the
#   scripts necessary to prepare the workflow for use (checkout, experiment 
#   setup, etc.).
#
# This script should be SOURCED to properly setup the environment.
#

HOMEobsforge="$(cd "$(dirname  "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd )"
source "${HOMEobsforge}/ush/detect_machine.sh"
source "${HOMEobsforge}/ush/module-setup.sh"
module use "${HOMEobsforge}/modulefiles"
module load "obsforge/${MACHINE_ID}"

# Detect the Python major.minor version
_regex="[0-9]+\.[0-9]+"
# shellcheck disable=SC2312
if [[ $(python --version) =~ ${_regex} ]]; then
    export PYTHON_VERSION="${BASH_REMATCH[0]}"
else
    echo "FATAL ERROR: Could not detect the python version"
    return 1
fi

###############################################################
# setup python path for ioda utilities
# TODO: a better solution should be created for setting paths to package python scripts
# shellcheck disable=SC2311
pyiodaPATH="${HOMEobsforge}/build/lib/python${PYTHON_VERSION}/"
# Add wxflow to PYTHONPATH
wxflowPATH="${HOMEobsforge}/ush/python"
PYTHONPATH="${PYTHONPATH:+${PYTHONPATH}:}${HOMEobsforge}/ush:${wxflowPATH}:${pyiodaPATH}"
export PYTHONPATH

export PYTHONPATH="${PYTHONPATH}:${HOMEobsforge}/build/lib/python${PYTHON_VERSION}/site-packages"

set +ue
