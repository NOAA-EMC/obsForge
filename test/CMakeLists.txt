#!/usr/bin/env python3
# (C) Copyright 2024 NOAA/NCEP/EMC.

# Download the test data if we don't have it.
# Code borrowed from NCEPLIB-bufr project...

#>>orig
#set(DOWNLOAD_URL "https://ftp.emc.ncep.noaa.gov/static_files/public/obsforge")
#set(FILE_COLLECTION "obsforge-0.0.1.tgz")
set(FILE_COLLECTION "obsforge-0.0.0.tgz")

## Get the test data if we don't have it.
if(NOT EXISTS "${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION}")
   message(STATUS "Downloading: " ${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION})
#  file(DOWNLOAD
#    ${DOWNLOAD_URL}/${FILE_COLLECTION}
#    ${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION}
#    SHOW_PROGRESS
#    STATUS download_status
#    INACTIVITY_TIMEOUT 30
#  )
#
#  list(GET download_status 0 download_status_num)
#
#  if(NOT download_status_num EQUAL 0 OR NOT EXISTS ${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION})
#    # Remove empty file if download doesn't complete
#    file(REMOVE ${CMAKE_CURRENT_BINARY_DIR}/${BUFR_TAR})
#    message(STATUS "Could not download test files, not building tests")
#    return()
#  endif()
#<<orig

#>>emily
  file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_COLLECTION} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
#<<emily

  add_custom_target(get_obsforge_test_data ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION})
  add_custom_command(
    TARGET get_obsforge_test_data
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_CURRENT_BINARY_DIR} tar xzf ${FILE_COLLECTION}
    COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_CURRENT_BINARY_DIR} rm -rf testdata
    COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_CURRENT_BINARY_DIR} rm -rf testoutput
    COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_CURRENT_BINARY_DIR} mv remote_data/testdata testdata
    COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_CURRENT_BINARY_DIR} mv remote_data/testoutput testoutput
    COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_CURRENT_BINARY_DIR} rm -rf remote_data)
endif()

message("Emily checking test/CMAKE_CURRENT_SOURCE_DIR - ${CMAKE_CURRENT_SOURCE_DIR}")
message("Emily checking test/CMAKE_CURRENT_BINARY_DIR - ${CMAKE_CURRENT_BINARY_DIR}")

# Function definitions

# The list of all test input config files, which need to be symlink from the build folder.
# (These symlink are created at the end of this file, to give CMake functions setting up certain
# tests a chance to append items to this list).

# List of test cases
list( APPEND test_case
  bufr2netcdf
  script2netcdf
  bufr_backend 
  script_backend 
)

# List of test input files 
list( APPEND test_input
  bufr_satwnd_amv_goes_mapping.yaml
  bufr_satwnd_amv_goes.py
  bufr_sfcsno_mapping.yaml
  bufr_sfcsno.py
)

# List of test configurations 
list( APPEND test_config
  bufr_bufr_backend_satwnd_amv_goes.yaml
  bufr_bufr_backend_satwnd_amv_goes_mpi4.yaml
  bufr_script_backend_satwnd_amv_goes.yaml
  bufr_script_backend_satwnd_amv_goes_mpi4.yaml
  bufr_bufr_backend_sfcsno.yaml
  bufr_bufr_backend_sfcsno_mpi2.yaml
  bufr_script_backend_sfcsno.yaml
  bufr_script_backend_sfcsno_mpi2.yaml
)

# create test intput directories and make links to the input files
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testinput)
foreach(FILENAME ${test_input})
  execute_process( COMMAND ${CMAKE_COMMAND} -E create_symlink
    ${CMAKE_CURRENT_SOURCE_DIR}/../sorc/spoc/dump/mapping/${FILENAME}
    ${CMAKE_CURRENT_BINARY_DIR}/testinput/${FILENAME} )
endforeach(FILENAME)

foreach(FILENAME ${test_config})
  execute_process( COMMAND ${CMAKE_COMMAND} -E create_symlink
    ${CMAKE_CURRENT_SOURCE_DIR}/testinput/${FILENAME}
    ${CMAKE_CURRENT_BINARY_DIR}/testinput/${FILENAME} )
endforeach(FILENAME)

# create test output directories
foreach(TESTCASE ${test_case})
  file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testrun/${TESTCASE})
endforeach(TESTCASE)

# =====================================================================
# Tests for sfcsno 
# =====================================================================
# Perform bufr2netcdf tests
ecbuild_add_test( TARGET  test_obsforge_sfcsno_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.sfcsno.tm00.bufr_d
                                                                 testinput/bufr_sfcsno_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.sfcsno.tm00.nc"
                          bufr2netcdf/gdas.t00z.sfcsno.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_sfcsno_bufr2netcdf_mpi2
                  MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.sfcsno.tm00.bufr_d
                                                                 testinput/bufr_sfcsno_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.sfcsno.tm00.nc"
                          bufr2netcdf/gdas.t00z.sfcsno.tm00.nc)

# Perform script2netcdf tests
ecbuild_add_test( TARGET  test_obsforge_sfcsno_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_sfcsno.py testdata/gdas.t00z.sfcsno.tm00.bufr_d
                                                             testinput/bufr_sfcsno_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.sfcsno.tm00.nc"
                          script2netcdf/gdas.t00z.sfcsno.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH}) 

ecbuild_add_test( TARGET  test_obsforge_sfcsno_script2netcdf_mpi2
                  MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_sfcsno.py testdata/gdas.t00z.sfcsno.tm00.bufr_d
                                                             testinput/bufr_sfcsno_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.sfcsno.tm00.nc"
                          script2netcdf/gdas.t00z.sfcsno.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

# Perform bufr_backend tests
ecbuild_add_test( TARGET  test_obsforge_sfcsno_bufr_backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr_backend_sfcsno.yaml"
                          bufr_backend/gdas.t00z.sfcsno.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_sfcsno_bufr_backend_mpi2
                  MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr_backend_sfcsno_mpi2.yaml"
                          bufr_backend/gdas.t00z.sfcsno.tm00_mpi2.nc)

# Perform script_backend tests
ecbuild_add_test( TARGET  test_obsforge_sfcsno_script_backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script_backend_sfcsno.yaml"
                          script_backend/gdas.t00z.sfcsno.tm00.nc)

 ecbuild_add_test( TARGET test_obsforge_sfcsno_script_backend_mpi2
                  MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script_backend_sfcsno_mpi2.yaml"
                          script_backend/gdas.t00z.sfcsno.tm00_mpi2.nc)

# =====================================================================
# Tests for satwnd
# =====================================================================
# Perform bufr2netcdf tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-16.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-17.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-18.tm00.nc)

# Perform bufr2netcdf tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_bufr2netdf_mpi4
   	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-16.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_bufr2netcdf_mpi8
   	          MPI 8
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-17.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_bufr2netcdf_mpi2
   	          MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-18.tm00.nc)		  

# Perform script2netcdf tests
#if (${BUILD_PYTHON_BINDINGS})
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_goes.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-16.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_goes.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-17.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_goes.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-18.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  

# Perform script2netcdf tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_script2netcdf_mpi4
	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_goes.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-16.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_script2netcdf_mpi8
	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_goes.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-17.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_script2netcdf_mpi2
                  MPI 2	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_goes.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-18.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})
#endif()
# Perform bufr_backend tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_bufr_backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr_backend_satwnd_amv_goes.yaml"
                          bufr_backend/gdas.t00z.satwnd.abi_goes-16.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_bufr_backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr_backend_satwnd_amv_goes.yaml"
                          bufr_backend/gdas.t00z.satwnd.abi_goes-17.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_bufr_backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr_backend_satwnd_amv_goes.yaml"
                          bufr_backend/gdas.t00z.satwnd.abi_goes-18.tm00.nc)

# Perform bufr_backend tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_bufr_backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr_backend_satwnd_amv_goes_mpi4.yaml"
                          bufr_backend/gdas.t00z.satwnd.abi_goes-16.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_bufr_backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr_backend_satwnd_amv_goes_mpi4.yaml"
                          bufr_backend/gdas.t00z.satwnd.abi_goes-17.tm00_mpi4.nc)


ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_bufr_backend_mpi4
                  MPI 4 	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr_backend_satwnd_amv_goes_mpi4.yaml"
                          bufr_backend/gdas.t00z.satwnd.abi_goes-18.tm00_mpi4.nc)

# Perform script_backend tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_script_backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script_backend_satwnd_amv_goes.yaml"
                          script_backend/gdas.t00z.satwnd.abi_goes-16.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_script_backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script_backend_satwnd_amv_goes.yaml"
                          script_backend/gdas.t00z.satwnd.abi_goes-17.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_script_backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script_backend_satwnd_amv_goes.yaml"
                          script_backend/gdas.t00z.satwnd.abi_goes-18.tm00.nc)

# Perform script_backend tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_script_backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script_backend_satwnd_amv_goes_mpi4.yaml"
                          script_backend/gdas.t00z.satwnd.abi_goes-16.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_script_backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script_backend_satwnd_amv_goes_mpi4.yaml"
                          script_backend/gdas.t00z.satwnd.abi_goes-17.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_script_backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script_backend_satwnd_amv_goes_mpi4.yaml"
                          script_backend/gdas.t00z.satwnd.abi_goes-18.tm00_mpi4.nc)
