#! /usr/bin/env bash

set -x
bindir=$1
srcdir=$2

type="aoddump"

# Set g-w HOMEobsforge
topdir=$(cd "$(dirname "$(readlink -f -n "${bindir}" )" )/../.." && pwd -P)
export HOMEobsforge=$topdir

export RUN_ENVIR="emc"
export NET="gcafs"
export RUN="gcdas"

current_utc_hour=$(date -u +%H)

if [[ ${current_utc_hour} -ge 8 ]]; then
    # Use today's date with 00 UTC cycle
    PDY=$(date -u +%Y%m%d)
    cyc="00"
else
    # Use yesterday's date with 18 UTC cycle
    PDY=$(date -u -d "yesterday" +%Y%m%d)
    cyc="18"
fi
export PDY
export cyc

export KEEPDATA="NO"
export COMROOT=$bindir/chem/run/
export DCOMROOT=${bindir}/chem/staged_input_obs
export DATAROOT=$bindir/chem/run/RUNDIRS/${RUN}.${PDY}${cyc}/
export OBSPROC_COMROOT=${bindir}/chem/staged_input_obs

export pid=${pid:-$$}
export jobid="aoddump.$pid"
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

export job="aoddump"

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
command: ${HOMEobsforge}/jobs/JOBSFORGE_GLOBAL_AOD_DUMP
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
    ${HOMEobsforge}/jobs/JOBSFORGE_GLOBAL_AOD_DUMP
fi
