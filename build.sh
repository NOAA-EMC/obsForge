#!/bin/bash

# build.sh
# 1 - determine host, load modules on supported hosts; proceed w/o otherwise
# 2 - configure; build; install
# 4 - optional, run unit tests

set -eu

echo "Start ... `date`"
dir_root="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $dir_root/ush/detect_machine.sh

# ==============================================================================
usage() {
  set +x
  echo
  echo "Usage: $0 -p <prefix> | -t <target> -h"
  echo
  echo "  -p  installation prefix <prefix>    DEFAULT: <none>"
  echo "  -t  target to build for <target>    DEFAULT: $MACHINE_ID"
  echo "  -c  additional CMake options        DEFAULT: <none>"
  echo "  -v  build with verbose output       DEFAULT: NO"
  echo "  -f  force a clean build             DEFAULT: NO"
  echo "  -h  display this message and quit"
  echo
  exit 1
}

# ==============================================================================

# Defaults:
INSTALL_PREFIX="${dir_root}/install"
CMAKE_INSTALL_LIBDIR="lib"
CMAKE_OPTS=""
BUILD_TARGET="${MACHINE_ID:-'localhost'}"
BUILD_VERBOSE="NO"
CLEAN_BUILD="NO"
COMPILER="${COMPILER:-intel}"

while getopts "p:t:c:hvdfa" opt; do
  case $opt in
    p)
      INSTALL_PREFIX=$OPTARG
      ;;
    t)
      BUILD_TARGET=$OPTARG
      ;;
    c)
      CMAKE_OPTS=$OPTARG
      ;;
    v)
      BUILD_VERBOSE=YES
      ;;
    f)
      CLEAN_BUILD=YES
      ;;
    h|\?|:)
      usage
      ;;
  esac
done

case ${BUILD_TARGET} in
  ursa | hera | orion | hercules | wcoss2 | noaacloud | gaeac5 | gaeac6 )
    echo "Building obsForge on $BUILD_TARGET"
    source $dir_root/ush/module-setup.sh
    module use $dir_root/modulefiles
    module load obsforge/$BUILD_TARGET.$COMPILER
    CMAKE_OPTS+=" -DMPIEXEC_EXECUTABLE=$MPIEXEC_EXEC -DMPIEXEC_NUMPROC_FLAG=$MPIEXEC_NPROC -DPython3_EXECUTABLE=$(which python3) -DON_NOAA_HPC=ON"
    module list
    ;;
  $(hostname))
    echo "Building obsForge on $BUILD_TARGET"
    ;;
  *)
    echo "Building obsForge on unknown target: $BUILD_TARGET"
    ;;
esac

CMAKE_OPTS+=" -DMACHINE=$BUILD_TARGET"

# TODO: Remove LD_LIBRARY_PATH line as soon as permanent solution is available
if [[ $BUILD_TARGET == 'wcoss2' ]]; then
  export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/opt/cray/pe/mpich/8.1.29/ofi/intel/2022.1/lib"
  export LMOD_MPI_NAME=cray-mpich
  export LMOD_MPI_VERSION=8.1.29-xhbciau
fi

BUILD_DIR=${BUILD_DIR:-$dir_root/build}
if [[ $CLEAN_BUILD == 'YES' ]]; then
  [[ -d ${BUILD_DIR} ]] && rm -rf ${BUILD_DIR}
fi
mkdir -p ${BUILD_DIR} && cd ${BUILD_DIR}

# Set INSTALL_PREFIX as CMake option
CMAKE_OPTS+=" -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}"

# Set CMAKE_INSTALL_LIBDIR as CMake option
CMAKE_OPTS+=" -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}"

# Configure
echo "Configuring ... `date`"
set -x
cmake \
  ${CMAKE_OPTS:-} \
  $dir_root/bundle
set +x

# Install
echo "Installing ... `date`"
set -x
make install -j ${BUILD_JOBS:-8} VERBOSE=${BUILD_VERBOSE:-}
set +x

echo "Finish ... `date`"
exit 0
