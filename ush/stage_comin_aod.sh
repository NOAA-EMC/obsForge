#!/bin/bash
# Stage COMIN for OBS processing

# print a warning message and exit if incorrect number of arguments are provided
if [[ $# -ne 4 ]]; then
    echo "FATAL ERROR: Incorrect number of arguments provided to stage_comin_aod.sh"
    echo "Usage: stage_comin_aod.sh <PDY> <cyc> <RUN> <DCOMROOT_OUT>"
    exit 1
fi

# get date, run, and output directory from input arguments
export PDY=$1
export cyc=$2
export RUN=$3
export DCOMROOT_OUT=$4

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
copy_or_download="download"

if [[ "${NRT}" == "YES" ]]; then
    if [[ "${MACHINE_ID}" == "wcoss2" ]]; then
        # use the operational DCOM directory
        export DCOMROOT="/lfs/h1/ops/prod/dcom/"
        copy_or_download="copy"
    else
        copy_or_download="download"
    fi
fi

# create the output directory if it does not exist
mkdir -p "${DCOMROOT_OUT}/${PDY}/jrr_aod"

if [[ "${copy_or_download}" == "copy" ]]; then
    # copy the files from DCOMROOT to DCOMROOT_OUT
    src_dir="${DCOMROOT}/${PDY}/jrr_aod"
    if [[ -d "${src_dir}" ]]; then
        echo "Copying files from ${src_dir} to ${DCOMROOT_OUT}/${PDY}/jrr_aod"
        cp -v "${src_dir}/"* "${DCOMROOT_OUT}/${PDY}/jrr_aod"
    else
        echo "FATAL ERROR: Source directory ${src_dir} does not exist."
        exit 1
    fi
else
    # download the files from NESDIS
    base_url="https://noaa-nesdis-n21-pds.s3.amazonaws.com?list-type=2&prefix=VIIRS-JRR-AOD"
    YYYY=${PDY:0:4}
    MM=${PDY:4:2}
    DD=${PDY:6:2}
    file_list=$(curl -s "${base_url}/${YYYY}/${MM}/${DD}/&delimiter=/" | grep -oP '(?<=<Key>).*?(?=</Key>)')
    bucket_url="https://noaa-nesdis-n21-pds.s3.amazonaws.com"
    file_count=0
    for file_name in ${file_list}; do
        if [[ $file_count -ge 10 ]]; then
            break
        fi
        ((file_count++))
        file_url="${bucket_url}/${file_name}"
        base_file_name=$(basename "${file_name}")
        wget -O "${DCOMROOT_OUT}/${PDY}/jrr_aod/${base_file_name}" "${file_url}"
        if [[ $? -ne 0 ]]; then
            echo "FATAL ERROR: Failed to download ${file_url}"
            exit 1
        fi
    done
fi
exit 0
