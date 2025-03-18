#! /usr/bin/env bash

###############################################################
if [[ "${DEBUG_WORKFLOW:-NO}" == "NO" ]]; then
    echo "Loading modules quietly..."
    set +x
fi

MODS="obsforge"

# Setup runtime environment by loading modules
ulimit_s=$( ulimit -S -s )

# Find module command and purge:
source "${HOMEobsforge}/ush/detect_machine.sh"
source "${HOMEobsforge}/ush/module-setup.sh"

# Load our modules:
module use "${HOMEobsforge}/modulefiles"

case "${MACHINE_ID}" in
  ("hera" | "orion" | "hercules" | "wcoss2" | "gaeac5" | "gaeac6")
    #TODO: Remove LMOD_TMOD_FIND_FIRST line when spack-stack on WCOSS2
    if [[ "${MACHINE_ID}" == "wcoss2" ]]; then
      export LMOD_TMOD_FIND_FIRST=yes
      # TODO: Add path to ObsForge libraries and cray-mpich as temporary patches
      # TODO: Remove LD_LIBRARY_PATH lines as soon as permanent solutions are available	
      export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${HOMEobsforge}/build/lib"
      export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/opt/cray/pe/mpich/8.1.19/ofi/intel/19.0/lib"
    fi
    module load "${MODS}/${MACHINE_ID}"
    ncdump=$( command -v ncdump )
    NETCDF=$( echo "${ncdump}" | cut -d " " -f 3 )
    export NETCDF
    ;;
  ("jet" | "s4" | "acorn")
    echo WARNING: OBSFORGE NOT SUPPORTED ON THIS PLATFORM
    ;;  
  *)
    echo "WARNING: UNKNOWN PLATFORM"
    ;;
esac

module list
pip list

# Add wxflow to PYTHONPATH
wxflowPATH="${HOMEobsforge}/ush/python"
PYTHONPATH="${PYTHONPATH:+${PYTHONPATH}:}${HOMEobsforge}/ush:${wxflowPATH}"
export PYTHONPATH

# Restore stack soft limit:
ulimit -S -s "${ulimit_s}"
unset ulimit_s

set_trace