#! /usr/bin/env bash

set -x
bindir=$1  # Root for data
srcdir=$2  # Root for code
opt_date=$3

type="atmosbufrdump"

# Set g-w HOMEobsforge
topdir=$(cd "$(dirname "$(readlink -f -n "${srcdir}" )" )" && pwd -P)
export HOMEobsforge=$topdir

export RUN_ENVIR="emc"
export NET="gfs"
export RUN="gdas"

usage() {
    echo "Usage: $0 bindir srcdir [YYYYMMDDHH]"
    echo "  Optional date must be in YYYYMMDDHH format (e.g. 2025120400)."
    exit 1
}

# If an optional date (YYYYMMDDHH) is provided, use its hour in lieu of current UTC hour.
# Otherwise fall back to using the current UTC hour as before.
if [[ -n "${opt_date}" ]]; then
    if [[ ! "${opt_date}" =~ ^[0-9]{10}$ ]]; then
        echo "ERROR: optional date must be in YYYYMMDDHH format"
        usage
    fi
    # Extract components from supplied date
    supplied_yyyymmdd=${opt_date:0:8}
    current_utc_hour=${opt_date:8:2}

    # Decide PDY and cyc relative to the supplied date
    PDY=${supplied_yyyymmdd}
    cyc=${current_utc_hour}
else
    current_utc_hour=$(date -u +%H)

    if ((10#${current_utc_hour} >= 8)); then
        # Use today's date with 00 UTC cycle
        PDY=$(date -u +%Y%m%d)
        cyc="00"
    else
        # Use yesterday's date with 18 UTC cycle
        PDY=$(date -u -d "yesterday" +%Y%m%d)
        cyc="18"
    fi
fi

export PDY
export cyc

export KEEPDATA="NO"
export COMROOT=$bindir/atmos/run/
export DCOMROOT=${bindir}/atmos/staged_input_obs
export DATAROOT=$bindir/atmos/run/RUNDIRS/${RUN}.${PDY}${cyc}/
export OBSPROC_COMROOT=${bindir}/atmos/staged_input_obs

export pid=${pid:-$$}
export jobid="atmosbufrdump.$pid"
export ACCOUNT="da-cpu"

export STRICT="NO"
source "${HOMEobsforge}/ush/preamble.sh"

source "${HOMEobsforge}/ush/detect_machine.sh"

###############################################################
# Source UFSDA workflow modules
. "${HOMEobsforge}/ush/load_obsforge_modules.sh"
status=$?
if [[ ${status} -ne 0 ]]; then
    exit "${status}"
fi

export job="atmosbufrdump"

# Create yaml with job configuration
memory="96Gb"
if [[ ${MACHINE_ID} == "gaeac6" ]]; then
    memory=0
fi
config_yaml="./config_${type}.yaml"
cat <<EOF > ${config_yaml}
machine: ${MACHINE_ID}
HOMEobsforge: ${HOMEobsforge}
job_name: ${type}
walltime: "00:30:00"
nodes: 1
ntasks_per_node: 24
threads_per_task: 1
memory: ${memory}
command: ${HOMEobsforge}/jobs/JOBSFORGE_GLOBAL_ATMOS_BUFR_DUMP
filename: submit_${type}.sh
EOF

SCHEDULER=$(echo `grep SCHEDULER ${HOMEobsforge}/test/hosts/${MACHINE_ID}.yaml | cut -d":" -f2` | tr -d ' ')

# Submit script to execute j-job
if [[ $SCHEDULER = 'slurm' ]]; then
    # Create script to execute j-job
    $HOMEobsforge/test/generate_job_script.py ${config_yaml}
    sbatch --export=ALL --wait submit_${type}.sh
elif [[ $SCHEDULER = 'pbspro' ]]; then
    # Create script to execute j-job
    $HOMEobsforge/test/generate_job_script.py ${config_yaml}
    qsub -V -W block=true submit_${type}.sh
else
    ${HOMEobsforge}/jobs/JOBSFORGE_GLOBAL_ATMOS_BUFR_DUMP
fi
