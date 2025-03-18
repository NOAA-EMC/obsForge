#! /usr/bin/env bash

source "${HOMEobsforge}/ush/preamble.sh"

###############################################################
# Source UFSDA workflow modules
. "${HOMEobsforge}/ush/load_obsforge_modules.sh"
status=$?
if [[ ${status} -ne 0 ]]; then
    exit "${status}"
fi

export job="aoddump"
export jobid="${job}.$$"

###############################################################
# Execute the JJOB
"${HOMEobsforge}/jobs/JOBSFORGE_GLOBAL_AOD_DUMP"
status=$?
exit "${status}"