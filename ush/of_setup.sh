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
export PYTHONPATH="${HOMEobsforge}/ush/python:$PYTHONPATH"

set +ue
