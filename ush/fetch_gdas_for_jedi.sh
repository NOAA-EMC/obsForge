#!/bin/bash
# This script fetches GDAS data for JEDI from HPSS
# and prepares it for conversion

# Get arguments from command line
if [[ "${#}" -ne 2 ]]; then
    echo "Usage: ${0} <YYYYMMDDHH> <output_directory>"
    exit 1
fi
YYYYMMDDHH=${1}
OUTPUT_DIR=${2}
# Create output directory if it doesn't exist
mkdir -p "${OUTPUT_DIR}"

# determine GDAS version based on date
if [[ "${YYYYMMDDHH}" -ge "2022112900" ]]; then
    gdas_version="v16.3"
elif [[ "${YYYYMMDDHH}" -ge "2022062700" ]]; then
    gdas_version="v16.2"
else
    gdas_version="prod"
fi

# break date and time into components
cycle_Y=${YYYYMMDDHH:0:4}
cycle_M=${YYYYMMDDHH:4:2}
cycle_D=${YYYYMMDDHH:6:2}
cyc=${YYYYMMDDHH:8:2}
# cycle_YM is YYYYMM
cycle_YM="${cycle_Y}${cycle_M}"
# PDY is YYYYMMDD
PDY="${cycle_Y}${cycle_M}${cycle_D}"

# HPSS path for the two tar files
hpss_path_root="/NCEPPROD/hpssprod/runhistory/rh${cycle_Y}/${cycle_YM}/${PDY}"
hpss_file="com_gfs_${gdas_version}_gdas.${PDY}_${cyc}.gdas.tar"
hpss_file_restart="com_gfs_${gdas_version}_gdas.${PDY}_${cyc}.gdas_restart.tar"

# get the names of the files to extract
radstat="./gdas.${PDY}/${cyc}/atmos/gdas.t${cyc}z.radstat"
cnvstat="./gdas.${PDY}/${cyc}/atmos/gdas.t${cyc}z.cnvstat"
oznstat="./gdas.${PDY}/${cyc}/atmos/gdas.t${cyc}z.oznstat"
abias="./gdas.${PDY}/${cyc}/atmos/gdas.t${cyc}z.abias"
abias_air="./gdas.${PDY}/${cyc}/atmos/gdas.t${cyc}z.abias_air"
abias_int="./gdas.${PDY}/${cyc}/atmos/gdas.t${cyc}z.abias_int"
abias_pc="./gdas.${PDY}/${cyc}/atmos/gdas.t${cyc}z.abias_pc"

# Fetch the tar files from HPSS
cd "${OUTPUT_DIR}"

echo "htar -xvf "${hpss_path_root}/${hpss_file}" ${cnvstat} ${oznstat}"
htar -xvf "${hpss_path_root}/${hpss_file}" ${cnvstat} ${oznstat}
echo "htar -xvf "${hpss_path_root}/${hpss_file_restart}" ${radstat} ${abias} ${abias_air} ${abias_int} ${abias_pc}"
htar -xvf "${hpss_path_root}/${hpss_file_restart}" ${radstat} ${abias} ${abias_air} ${abias_int} ${abias_pc}

echo "GDAS data for ${YYYYMMDDHH} has been successfully fetched and stored in ${OUTPUT_DIR}."
# End of script
exit 0
