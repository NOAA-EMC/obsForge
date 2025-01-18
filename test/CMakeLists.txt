#!/usr/bin/env python3
# (C) Copyright 2024 NOAA/NCEP/EMC.

# Download the test data if we don't have it.
# Code borrowed from NCEPLIB-bufr project...

set(DOWNLOAD_URL "https://ftp.emc.ncep.noaa.gov/static_files/public/obsforge")
set(FILE_COLLECTION "obsforge-0.0.0.tgz")
#set(FILE_COLLECTION "obsforge-0.0.1.tgz")

# Get the test data if we don't have it.
if(NOT EXISTS "${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION}")
   message(STATUS "Downloading: " ${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION})
  file(DOWNLOAD
    ${DOWNLOAD_URL}/${FILE_COLLECTION}
    ${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION}
    SHOW_PROGRESS
    STATUS download_status
    INACTIVITY_TIMEOUT 30
  )

  list(GET download_status 0 download_status_num)

  if(NOT download_status_num EQUAL 0 OR NOT EXISTS ${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION})
    # Remove empty file if download doesn't complete
    file(REMOVE ${CMAKE_CURRENT_BINARY_DIR}/${BUFR_TAR})
    message(STATUS "Could not download test files, not building tests")
    return()
  endif()

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

# Function definitions

# The list of all test input config files, which need to be symlink from the build folder.
# (These symlink are created at the end of this file, to give CMake functions setting up certain
# tests a chance to append items to this list).

# List of test cases
list( APPEND test_case
  bufr2netcdf
  script2netcdf
  bufr4backend 
  script4backend 
)

# List of test input files 
list( APPEND test_input
  bufr_satwnd_amv_abi_mapping.yaml
  bufr_satwnd_amv_abi.py
  bufr_satwnd_amv_seviri_mapping.yaml
  bufr_satwnd_amv_seviri.py
  bufr_satwnd_amv_viirs_mapping.yaml
  bufr_satwnd_amv_viirs.py
  bufr_satwnd_amv_leogeo_mapping.yaml
  bufr_satwnd_amv_leogeo.py
  bufr_sfcsno_mapping.yaml
  bufr_sfcsno.py
)

# List of test configurations 
list( APPEND test_config
  bufr_bufr4backend_satwnd_amv_abi.yaml
  bufr_bufr4backend_satwnd_amv_abi_mpi4.yaml
  bufr_script4backend_satwnd_amv_abi.yaml
  bufr_script4backend_satwnd_amv_abi_mpi4.yaml
  bufr_bufr4backend_satwnd_amv_seviri.yaml
  bufr_bufr4backend_satwnd_amv_seviri_mpi4.yaml
  bufr_script4backend_satwnd_amv_seviri.yaml
  bufr_script4backend_satwnd_amv_seviri_mpi4.yaml
  bufr_bufr4backend_satwnd_amv_viirs.yaml
  bufr_bufr4backend_satwnd_amv_viirs_mpi4.yaml
  bufr_script4backend_satwnd_amv_viirs.yaml
  bufr_script4backend_satwnd_amv_viirs_mpi4.yaml
  bufr_bufr4backend_satwnd_amv_leogeo.yaml
  bufr_bufr4backend_satwnd_amv_leogeo_mpi4.yaml
  bufr_script4backend_satwnd_amv_leogeo.yaml
  bufr_script4backend_satwnd_amv_leogeo_mpi4.yaml
  bufr_bufr4backend_sfcsno.yaml
  bufr_bufr4backend_sfcsno_mpi2.yaml
  bufr_script4backend_sfcsno.yaml
  bufr_script4backend_sfcsno_mpi2.yaml
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

# Perform bufr4backend tests
ecbuild_add_test( TARGET  test_obsforge_sfcsno_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_sfcsno.yaml"
                          bufr4backend/gdas.t00z.sfcsno.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_sfcsno_bufr4backend_mpi2
                  MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_sfcsno_mpi2.yaml"
                          bufr4backend/gdas.t00z.sfcsno.tm00_mpi2.nc)

# Perform script4backend tests
ecbuild_add_test( TARGET  test_obsforge_sfcsno_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_sfcsno.yaml"
                          script4backend/gdas.t00z.sfcsno.tm00.nc)

 ecbuild_add_test( TARGET test_obsforge_sfcsno_script4backend_mpi2
                  MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_sfcsno_mpi2.yaml"
                          script4backend/gdas.t00z.sfcsno.tm00_mpi2.nc)

# =====================================================================
# Tests for satwnd abi
# =====================================================================
# Perform bufr2netcdf tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_abi_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-16.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_abi_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-17.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_abi_mapping.yaml
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
                                                                 testinput/bufr_satwnd_amv_abi_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-16.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_bufr2netcdf_mpi8
   	          MPI 8
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_abi_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-17.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_bufr2netcdf_mpi2
   	          MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_abi_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-18.tm00.nc)		  

# Perform script2netcdf tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_abi.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_abi_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-16.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_abi.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_abi_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-17.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_abi.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_abi_mapping.yaml
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
                          "testinput/bufr_satwnd_amv_abi.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_abi_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-16.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_script2netcdf_mpi8
	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_abi.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_abi_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-17.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_script2netcdf_mpi2
                  MPI 2	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_abi.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_abi_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-18.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

# Perform bufr4backend tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_abi.yaml"
                          bufr4backend/gdas.t00z.satwnd.abi_goes-16.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_abi.yaml"
                          bufr4backend/gdas.t00z.satwnd.abi_goes-17.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_abi.yaml"
                          bufr4backend/gdas.t00z.satwnd.abi_goes-18.tm00.nc)

# Perform bufr4backend tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_bufr4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_abi_mpi4.yaml"
                          bufr4backend/gdas.t00z.satwnd.abi_goes-16.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_bufr4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_abi_mpi4.yaml"
                          bufr4backend/gdas.t00z.satwnd.abi_goes-17.tm00_mpi4.nc)


ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_bufr4backend_mpi4
                  MPI 4 	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_abi_mpi4.yaml"
                          bufr4backend/gdas.t00z.satwnd.abi_goes-18.tm00_mpi4.nc)

# Perform script4backend tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_abi.yaml"
                          script4backend/gdas.t00z.satwnd.abi_goes-16.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_abi.yaml"
                          script4backend/gdas.t00z.satwnd.abi_goes-17.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_abi.yaml"
                          script4backend/gdas.t00z.satwnd.abi_goes-18.tm00.nc)

# Perform script4backend tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_script4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_abi_mpi4.yaml"
                          script4backend/gdas.t00z.satwnd.abi_goes-16.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_script4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf

                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_abi_mpi4.yaml"
                          script4backend/gdas.t00z.satwnd.abi_goes-17.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_script4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_abi_mpi4.yaml"
                          script4backend/gdas.t00z.satwnd.abi_goes-18.tm00_mpi4.nc)

# =====================================================================
# Tests for satwnd seviri
# =====================================================================
# Perform bufr2netcdf tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m8_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.seviri_m8.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m9_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.seviri_m9.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m10_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.seviri_m10.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m11_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.seviri_m11.tm00.nc)

# Perform bufr2netcdf tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m8_bufr2netdf_mpi4
   	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.seviri_m8.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m9_bufr2netcdf_mpi8
   	          MPI 8
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.seviri_m9.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m10_bufr2netcdf_mpi2
   	          MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.seviri_m10.tm00.nc)		  

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m11_bufr2netcdf_mpi6
   	          MPI 6 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.seviri_m11.tm00.nc)		  

# Perform script2netcdf tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m8_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_seviri.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.seviri_m8.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m9_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_seviri.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.seviri_m9.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m10_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_seviri.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.seviri_m10.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m11_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_seviri.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.seviri_m11.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  
# Perform script2netcdf tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m8_script2netcdf_mpi4
	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_seviri.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.seviri_m8.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m9_script2netcdf_mpi8
	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_seviri.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.seviri_m9.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m10_script2netcdf_mpi2
                  MPI 2	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_seviri.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.seviri_m10.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m11_script2netcdf_mpi6
                  MPI 6 	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_seviri.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_seviri_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.seviri_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.seviri_m11.tm00.nc)

# Perform bufr4backend tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m8_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_seviri.yaml"
                          bufr4backend/gdas.t00z.satwnd.seviri_m8.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m9_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_seviri.yaml"
                          bufr4backend/gdas.t00z.satwnd.seviri_m9.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m10_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_seviri.yaml"
                          bufr4backend/gdas.t00z.satwnd.seviri_m10.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m11_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_seviri.yaml"
                          bufr4backend/gdas.t00z.satwnd.seviri_m11.tm00.nc)

# Perform bufr4backend tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m8_bufr4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_seviri_mpi4.yaml"
                          bufr4backend/gdas.t00z.satwnd.seviri_m8.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m9_bufr4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_seviri_mpi4.yaml"
                          bufr4backend/gdas.t00z.satwnd.seviri_m9.tm00_mpi4.nc)


ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m10_bufr4backend_mpi4
                  MPI 4 	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_seviri_mpi4.yaml"
                          bufr4backend/gdas.t00z.satwnd.seviri_m10.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m11_bufr4backend_mpi4
                  MPI 4 	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_seviri_mpi4.yaml"
                          bufr4backend/gdas.t00z.satwnd.seviri_m11.tm00_mpi4.nc)

# Perform script4backend tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m8_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_seviri.yaml"
                          script4backend/gdas.t00z.satwnd.seviri_m8.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m9_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_seviri.yaml"
                          script4backend/gdas.t00z.satwnd.seviri_m9.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m10_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_seviri.yaml"
                          script4backend/gdas.t00z.satwnd.seviri_m10.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m11_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_seviri.yaml"
                          script4backend/gdas.t00z.satwnd.seviri_m11.tm00.nc)

# Perform script4backend tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m8_script4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_seviri_mpi4.yaml"
                          script4backend/gdas.t00z.satwnd.seviri_m8.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m9_script4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_seviri_mpi4.yaml"
                          script4backend/gdas.t00z.satwnd.seviri_m9.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m10_script4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_seviri_mpi4.yaml"
                          script4backend/gdas.t00z.satwnd.seviri_m10.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_seviri_m11_script4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_seviri_mpi4.yaml"
                          script4backend/gdas.t00z.satwnd.seviri_m11.tm00_mpi4.nc)

# =====================================================================
# Tests for satwnd viirs 
# =====================================================================
# Perform bufr2netcdf tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_npp_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_viirs_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.viirs_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.viirs_npp.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n20_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_viirs_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.viirs_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.viirs_n20.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n21_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_viirs_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.viirs_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.viirs_n21.tm00.nc)

# Perform bufr2netcdf tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_npp_bufr2netdf_mpi4
   	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_viirs_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.viirs_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.viirs_npp.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n20_bufr2netcdf_mpi8
   	          MPI 8
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_viirs_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.viirs_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.viirs_n20.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n21_bufr2netcdf_mpi2
   	          MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_viirs_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.viirs_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.viirs_n21.tm00.nc)		  

# Perform script2netcdf tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_npp_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_viirs.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_viirs_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.viirs_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.viirs_npp.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n20_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_viirs.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_viirs_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.viirs_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.viirs_n20.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n21_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_viirs.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_viirs_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.viirs_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.viirs_n21.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  

# Perform script2netcdf tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_npp_script2netcdf_mpi4
	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_viirs.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_viirs_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.viirs_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.viirs_npp.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n20_script2netcdf_mpi8
	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_viirs.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_viirs_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.viirs_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.viirs_n20.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n21_script2netcdf_mpi2
                  MPI 2	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_viirs.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_viirs_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.viirs_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.viirs_n21.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

# Perform bufr4backend tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_npp_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_viirs.yaml"
                          bufr4backend/gdas.t00z.satwnd.viirs_npp.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n20_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_viirs.yaml"
                          bufr4backend/gdas.t00z.satwnd.viirs_n20.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n21_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_viirs.yaml"
                          bufr4backend/gdas.t00z.satwnd.viirs_n21.tm00.nc)

# Perform bufr4backend tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_npp_bufr4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_viirs_mpi4.yaml"
                          bufr4backend/gdas.t00z.satwnd.viirs_npp.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n20_bufr4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_viirs_mpi4.yaml"
                          bufr4backend/gdas.t00z.satwnd.viirs_n20.tm00_mpi4.nc)


ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n21_bufr4backend_mpi4
                  MPI 4 	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_viirs_mpi4.yaml"
                          bufr4backend/gdas.t00z.satwnd.viirs_n21.tm00_mpi4.nc)

# Perform script4backend tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_npp_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_viirs.yaml"
                          script4backend/gdas.t00z.satwnd.viirs_npp.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n20_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_viirs.yaml"
                          script4backend/gdas.t00z.satwnd.viirs_n20.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n21_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_viirs.yaml"
                          script4backend/gdas.t00z.satwnd.viirs_n21.tm00.nc)

# Perform script4backend tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_npp_script4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_viirs_mpi4.yaml"
                          script4backend/gdas.t00z.satwnd.viirs_npp.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n20_script4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_viirs_mpi4.yaml"
                          script4backend/gdas.t00z.satwnd.viirs_n20.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_viirs_n21_script4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_viirs_mpi4.yaml"
                          script4backend/gdas.t00z.satwnd.viirs_n21.tm00_mpi4.nc)

# =====================================================================
# Tests for satwnd leogeo 
# =====================================================================
# Perform bufr2netcdf tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_leogeo_multi_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_leogeo_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.leogeo_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.leogeo_multi.tm00.nc)

# Perform bufr2netcdf tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_leogeo_multi_bufr2netdf_mpi4
   	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_leogeo_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.leogeo_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.leogeo_multi.tm00.nc)

# Perform script2netcdf tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_leogeo_multi_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_leogeo.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_leogeo_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.leogeo_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.leogeo_multi.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  
# Perform script2netcdf tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_leogeo_multi_script2netcdf_mpi4
	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_leogeo.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_leogeo_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.leogeo_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.leogeo_multi.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

# Perform bufr4backend tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_leogeo_multi_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_leogeo.yaml"
                          bufr4backend/gdas.t00z.satwnd.leogeo_multi.tm00.nc)


# Perform bufr4backend tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_leogeo_multi_bufr4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_leogeo_mpi4.yaml"
                          bufr4backend/gdas.t00z.satwnd.leogeo_multi.tm00_mpi4.nc)

# Perform script4backend tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_leogeo_multi_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_leogeo.yaml"
                          script4backend/gdas.t00z.satwnd.leogeo_multi.tm00.nc)

# Perform script4backend tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_leogeo_multi_script4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_leogeo_mpi4.yaml"
                          script4backend/gdas.t00z.satwnd.leogeo_multi.tm00_mpi4.nc)
