# ------------------------------------------------------------------------- #
# obsForge Bundle             #
# ------------------------------------------------------------------------- #

# Check for minimim cmake requirement
cmake_minimum_required( VERSION 3.20 FATAL_ERROR )

find_package(ecbuild 3.5 REQUIRED HINTS ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../ecbuild)

project(obsForge-bundle VERSION 1.0.0 LANGUAGES C CXX Fortran )

include(GNUInstallDirs)
enable_testing()

# Build type.
if(NOT CMAKE_BUILD_TYPE MATCHES "^(Debug|Release|RelWithDebInfo|MinSizeRel)$")
  message(STATUS "Setting build type to 'Release' as none was specified.")
  set(CMAKE_BUILD_TYPE
      "Release"
      CACHE STRING "Choose the type of build." FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
                                               "MinSizeRel" "RelWithDebInfo")
endif()

# Find dependencies.
find_package(Python3 REQUIRED COMPONENTS Interpreter)

# Include ecbuild_bundle macro
include( ecbuild_bundle )

# Enable MPI
set( ENABLE_MPI ON CACHE BOOL "Compile with MPI" )

# Initialize bundle
# -----------------
ecbuild_bundle_initialize()

option( ON_NOAA_HPC "Build with NOAA HPC specific settings" OFF )

# jedi-cmake
ecbuild_bundle( PROJECT jedicmake SOURCE "../sorc/jedicmake" )
include( jedicmake/cmake/Functions/git_functions.cmake )

# turn off optional OOPS toy models
option( ENABLE_LORENZ95_MODEL "Build LORENZ95 toy model" OFF )
option( ENABLE_QG_MODEL "Build QG toy model" OFF )

# EMC BUFR-query library
option( SKIP_DOWNLOAD_TEST_DATA "Skip download of test data tarball" OFF )
ecbuild_bundle( PROJECT bufr-query SOURCE "../sorc/bufr-query" )

# Core JEDI repositories
ecbuild_bundle( PROJECT oops SOURCE "../sorc/oops" )
#ecbuild_bundle( PROJECT vader SOURCE "../sorc/vader" )
#ecbuild_bundle( PROJECT saber SOURCE "../sorc/saber" )
option(ENABLE_IODA_DATA "Obtain ioda test data from ioda-data repository (vs tarball)" ON)
ecbuild_bundle( PROJECT ioda SOURCE "../sorc/ioda" )

# Build JEDI/DA or other peripherals
ecbuild_bundle( PROJECT obsforge-utils SOURCE "../utils" )
ecbuild_bundle( PROJECT da-utils SOURCE "../sorc/da-utils" )

# obsForge
ecbuild_bundle( PROJECT obsForge SOURCE "../")

# Finalize bundle
# ---------------
ecbuild_bundle_finalize()
