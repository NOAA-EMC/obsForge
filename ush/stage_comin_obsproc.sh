#!/bin/bash
# Stage COMIN for OBS processing

# print a warning message and exit if incorrect number of arguments are provided
if [[ $# -ne 4 ]]; then
    echo "FATAL ERROR: Incorrect number of arguments provided to stage_comin_obsproc.sh"
    echo "Usage: stage_comin_obsproc.sh <PDY> <cyc> <RUN> <COMOUT_OBSPROC>"
    exit 1
fi

# get date, run, and output directory from input arguments
export PDY=$1
export cyc=$2
export RUN=$3
export COMOUT_OBSPROC=$4

# check if PDY and cyc are within the past 7 days
current_date=$(date -u +"%Y%m%d")
date_diff=$(( ( $(date -u -d "${current_date}" +%s) - $(date -u -d "${PDY}" +%s) ) / 86400 ))

if [[ ${date_diff} -lt 0 || ${date_diff} -gt 7 ]]; then
    echo "Using ${PDY} and ${cyc} which are outside the past 7 days."
    NRT="NO"
else
    NRT="YES"
fi

HOMEobsforge="$(cd "$(dirname  "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd )"
# run detect machine script
source "${HOMEobsforge}/ush/detect_machine.sh"

# different behavior based on machine
copy_or_download="copy"
obsproc_ver=v1.2
if [[ "${MACHINE_ID}" == "wcoss2" ]]; then
    if [[ "${NRT}" == "YES" ]]; then
        # use the operational obsproc COM directory
        export COMIN_OBSPROC_ROOT="/lfs/h1/ops/prod/com/obsproc/${obsproc_ver}"
    else
        # use the GDA
        export COMIN_OBSPROC_ROOT="/lfs/h2/emc/dump/noscrub/dump"
    fi
elif [[ "${MACHINE_ID}" == "hera" || "${MACHINE_ID}" == "ursa" ]]; then
    export COMIN_OBSPROC_ROOT="/scratch3/NCEPDEV/global/role.glopara/dump"
elif [[ "${MACHINE_ID}" == "orion" || "${MACHINE_ID}" == "hercules" ]]; then
    export COMIN_OBSPROC_ROOT="/work/noaa/rstprod/dump"
elif [[ "${MACHINE_ID}" == "gaeac6" ]]; then
    export COMIN_OBSPROC_ROOT="/gpfs/f6/drsa-precip3/world-shared/role.glopara/dump"
else
    echo "On an unsupported machine, will try to download from NOMADS"
    copy_or_download="download"
fi

# create the output directory if it does not exist
# assume that it is an atmos processing run
mkdir -p "${COMOUT_OBSPROC}/${RUN}.${PDY}/${cyc}/atmos"

if [[ "${copy_or_download}" == "copy" ]]; then
    # copy the files from COMIN_OBSPROC_ROOT to COMOUT_OBSPROC
    src_dir="${COMIN_OBSPROC_ROOT}/${RUN}.${PDY}/${cyc}/atmos"
    if [[ -d "${src_dir}" ]]; then
        echo "Copying files from ${src_dir} to ${COMOUT_OBSPROC}/${RUN}.${PDY}/${cyc}/atmos/"
        cp -v "${src_dir}/"* "${COMOUT_OBSPROC}/${RUN}.${PDY}/${cyc}/atmos/"
    else
        echo "FATAL ERROR: Source directory ${src_dir} does not exist."
        exit 1
    fi
else
    # download the files from NOMADS
    base_url="https://nomads.ncep.noaa.gov/pub/data/nccf/com/obsproc/prod/"
    filter_pattern="href=\"${RUN}\.t${cyc}z\.[^']*\""
    file_list=$(curl -s "${base_url}/${RUN}.${PDY}/" | grep -o "${filter_pattern}" | tr ' ' '\n' | cut -d'"' -f2)
    for file_name in ${file_list}; do
        # skip the large file
        if [[ "${file_name}" == *"sstvcw"* ]]; then
            continue
        fi
        file_url="${base_url}/${RUN}.${PDY}/${file_name}"
        wget -O "${COMOUT_OBSPROC}/${RUN}.${PDY}/${cyc}/atmos/${file_name}" "${file_url}"
        if [[ $? -ne 0 ]]; then
            echo "FATAL ERROR: Failed to download ${file_url}"
            exit 1
        fi
    done
    # if anything ends in .nr, remove that suffix from the file
    for file_path in "${COMOUT_OBSPROC}/${RUN}.${PDY}/${cyc}/atmos/"*.nr; do
        if [[ -e "${file_path}" ]]; then
            mv "${file_path}" "${file_path%.nr}"
        fi
    done
fi
exit 0
