# CMake configuration for automated testing
cmake_minimum_required(VERSION 3.10)

project(obsforge_tests)

# =============================================================
# Function to add tests for satellite observations dynamically
# =============================================================
function(add_obsforge_test_satobs target_prefix obs_type obs_source sensor_name sat_name test_config mpi_count)

    # bufr2netcdf
    if(test_config STREQUAL "bufr2netcdf") 
        set(target_name "${target_prefix}_${obs_type}_${sat_name}_${test_config}")
        set(env_args "")
        set(command_args
            "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.${obs_source}.tm00.bufr_d
            testinput/bufr_${obs_type}_mapping.yaml
            testrun/${test_config}/gdas.t00z.${obs_source}.${sensor_name}_{splits/satId}.tm00.nc"
            ${test_config}/gdas.t00z.${obs_source}.${sensor_name}_${sat_name}.tm00.nc
        )
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # script2netcdf
    elseif(test_config STREQUAL "script2netcdf")
        set(target_name "${target_prefix}_${obs_type}_${sat_name}_${test_config}")
        set(env_args "")
        set(command_args
            "testinput/bufr_${obs_type}.py testdata/gdas.t00z.${obs_source}.tm00.bufr_d
            testinput/bufr_${obs_type}_mapping.yaml
            testrun/${test_config}/gdas.t00z.${obs_source}.${sensor_name}_{splits/satId}.tm00.nc"
            ${test_config}/gdas.t00z.${obs_source}.${sensor_name}_${sat_name}.tm00.nc
        )
        set(env_args
            "ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH}"
        )
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # bufr4backend
    elseif(test_config STREQUAL "bufr4backend")
        set(target_name "${target_prefix}_${obs_type}_${sat_name}_${test_config}")
        set(env_args "")
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.${sensor_name}_${sat_name}.tm00_mpi${mpi_count}.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.${sensor_name}_${sat_name}.tm00.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # script4backend
    elseif(test_config STREQUAL "script4backend")
        set(target_name "${target_prefix}_${obs_type}_${sat_name}_${test_config}")
        set(env_args "")
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.${sensor_name}_${sat_name}.tm00_mpi${mpi_count}.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.${sensor_name}_${sat_name}.tm00.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    else()
	    message(Status "Unknown test configuration")     
    endif()

endfunction()

# ================================================================================
# Function to add tests for conventional observations from bufr dump dynamically
# ================================================================================
function(add_obsforge_test_convobs target_prefix obs_type obs_source test_config mpi_count)

    # bufr2netcdf
    if(test_config STREQUAL "bufr2netcdf") 
        set(target_name "${target_prefix}_${obs_type}_${test_config}")
        set(env_args "")
        set(command_args
            "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.${obs_source}.tm00.bufr_d
            testinput/bufr_${obs_type}_mapping.yaml
            testrun/${test_config}/gdas.t00z.${obs_source}.tm00.nc"
            ${test_config}/gdas.t00z.${obs_source}.tm00.nc
        )
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # script2netcdf
    elseif(test_config STREQUAL "script2netcdf")
        set(target_name "${target_prefix}_${obs_type}_${test_config}")
        set(env_args "")
        set(command_args
            "testinput/bufr_${obs_type}.py testdata/gdas.t00z.${obs_source}.tm00.bufr_d
            testinput/bufr_${obs_type}_mapping.yaml
            testrun/${test_config}/gdas.t00z.${obs_source}.tm00.nc"
            ${test_config}/gdas.t00z.${obs_source}.tm00.nc
        )
        set(env_args
            "ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH}"
        )
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # bufr4backend
    elseif(test_config STREQUAL "bufr4backend")
        set(target_name "${target_prefix}_${obs_type}_${test_config}")
        set(env_args "")
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.tm00_mpi${mpi_count}.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.tm00.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # script4backend
    elseif(test_config STREQUAL "script4backend")
        set(target_name "${target_prefix}_${obs_type}_${test_config}")
        set(env_args "")
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.tm00_mpi${mpi_count}.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.tm00.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    else()
	    message(Status "Unknown test configuration")     
    endif()

endfunction()

# ================================================================================
# Function to add tests for conventional observations from prepbufr dynamically
# ================================================================================
function(add_obsforge_test_convobs_prepbufr target_prefix obs_type obs_source test_config mpi_count)

    # bufr2netcdf
    if(test_config STREQUAL "bufr2netcdf") 
        set(target_name "${target_prefix}_${obs_type}_${test_config}")
        set(env_args "")
	if(obs_source STREQUAL "acft_profiles")
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.prepbufr.${obs_source}
                testinput/bufr_${obs_type}_mapping.yaml
                testrun/${test_config}/gdas.t00z.${obs_type}.tm00.nc"
                ${test_config}/gdas.t00z.${obs_type}.tm00.nc
            )
	else()
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.prepbufr
                testinput/bufr_${obs_type}_mapping.yaml
                testrun/${test_config}/gdas.t00z.${obs_type}.tm00.nc"
                ${test_config}/gdas.t00z.${obs_type}.tm00.nc
            )
        endif()
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # script2netcdf
    elseif(test_config STREQUAL "script2netcdf")
        set(target_name "${target_prefix}_${obs_type}_${test_config}")
        set(env_args "")
        if(obs_source STREQUAL "acft_profiles")
            set(command_args
                "testinput/bufr_${obs_type}.py testdata/gdas.t00z.prepbufr.${obs_source}
                testinput/bufr_${obs_type}_mapping.yaml
                testrun/${test_config}/gdas.t00z.${obs_type}.tm00.nc
		2021080100"
                ${test_config}/gdas.t00z.${obs_type}.tm00.nc
            )
        else()
            set(command_args
                "testinput/bufr_${obs_type}.py testdata/gdas.t00z.prepbufr
                testinput/bufr_${obs_type}_mapping.yaml
                testrun/${test_config}/gdas.t00z.${obs_type}.tm00.nc
		2021080100"
                ${test_config}/gdas.t00z.${obs_type}.tm00.nc
            )
        endif()
        set(env_args
            "ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH}"
        )
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # bufr4backend
    elseif(test_config STREQUAL "bufr4backend")
        set(target_name "${target_prefix}_${obs_type}_${test_config}")
        set(env_args "")
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.tm00_mpi${mpi_count}.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}.yaml"
                ${test_config}/gdas.t00z.${obs_type}.tm00.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # script4backend
    elseif(test_config STREQUAL "script4backend")
        set(target_name "${target_prefix}_${obs_type}_${test_config}")
        set(env_args "")
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.tm00_mpi${mpi_count}.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.tm00.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    else()
	    message(Status "Unknown test configuration")
    endif()

endfunction()

# ===============================================
# Set test parameters for satellite observations
# ===============================================

set(satobs_types "satwnd_amv_abi" "satwnd_amv_seviri" "satwnd_amv_viirs" "satwnd_amv_leogeo")
set(test_configs "bufr2netcdf" "script2netcdf" "bufr4backend" "script4backend")
set(mpi_counts_convobs "0" "2")
set(mpi_counts_satobs "0" "4")

# Associate nested lists (satellites) with each observation type
set_property(GLOBAL PROPERTY satwnd_amv_abi_satellites "goes-16;goes-17;goes-18")
set_property(GLOBAL PROPERTY satwnd_amv_seviri_satellites "m8;m9;m10;m11")
set_property(GLOBAL PROPERTY satwnd_amv_viirs_satellites "npp;n20;n21")
set_property(GLOBAL PROPERTY satwnd_amv_leogeo_satellites "multi")

# Get the length of the satobs_types list
list(LENGTH satobs_types satobs_types_length)
math(EXPR satobs_types_last_index "${satobs_types_length} - 1")

# ======================================================
# Generate tests for satellite observations dynamically
# ======================================================
foreach(index RANGE 0 ${satobs_types_last_index})
    list(GET satobs_types ${index} satobs_type)
    
    # Extract components from satobs_type
    string(REPLACE "_" ";" satobs_type_parts "${satobs_type}")
    list(GET satobs_type_parts 0 obs_source)    # Extract "satwnd"
    list(GET satobs_type_parts 2 sensor_name)   # Extract "abi"

    # Retrieve the nested list of satellite IDs for the current observation type
    get_property(satellites GLOBAL PROPERTY ${satobs_type}_satellites)
    foreach(sat_name ${satellites})
	foreach(test_config ${test_configs})
	    foreach(mpi_count ${mpi_counts_satobs})
                add_obsforge_test_satobs("test_obsforge_satobs" ${satobs_type} ${obs_source} ${sensor_name} ${sat_name} ${test_config} ${mpi_count})
            endforeach()
        endforeach()
    endforeach()
endforeach()

# ==================================================================
# Set test parameters for conventional observations from bufr dump
# ==================================================================
set(convobs_types "sfcsno" )

# Get the length of the convobs_types list
list(LENGTH convobs_types convobs_types_length)
math(EXPR convobs_types_last_index "${convobs_types_length} - 1")

# =========================================================
# Generate tests for conventional observations dynamically
# =========================================================
foreach(index RANGE 0 ${convobs_types_last_index})
    list(GET convobs_types ${index} convobs_type)

    set(obs_source ${convobs_type})
    foreach(test_config ${test_configs})
        foreach(mpi_count ${mpi_counts_convobs})
            add_obsforge_test_convobs("test_obsforge_convobs" ${convobs_type} ${obs_source} ${test_config} ${mpi_count})
        endforeach()
    endforeach()
endforeach()


# ==================================================================
# Set test parameters for conventional observations from prepbufr 
# ==================================================================
set(convobs_prepbufr_types "adpsfc_prepbufr" "sfcshp_prepbufr" "acft_profiles_prepbufr" "adpupa_prepbufr")
set(mpi_counts_convobs_prepbufr "0" "4")

# Get the length of the convobs_prepbufr_types list
list(LENGTH convobs_prepbufr_types convobs_prepbufr_types_length)
math(EXPR convobs_prepbufr_types_last_index "${convobs_prepbufr_types_length} - 1")

# =================================================================
# Generate tests for conventional prepbufr observations dynamically
# =================================================================
foreach(index RANGE 0 ${convobs_prepbufr_types_last_index})
    list(GET convobs_prepbufr_types ${index} convobs_prepbufr_type)

    # Extract components from convobs_prepbufr_type
    string(REPLACE "_prepbufr" ";prepbufr" convobs_prepbufr_type_parts "${convobs_prepbufr_type}")
    list(GET convobs_prepbufr_type_parts 0 obs_source)

    set(obs_source ${convobs_prepbufr_type})
    foreach(test_config ${test_configs})
        foreach(mpi_count ${mpi_counts_convobs_prepbufr})
            add_obsforge_test_convobs_prepbufr("test_obsforge_convobs_prepbufr" ${convobs_prepbufr_type} ${obs_source} ${test_config} ${mpi_count})
        endforeach()
    endforeach()
endforeach()

# =================================================================================
# Create necessary test directories and links to required data and files
# =================================================================================
# testdata   : input bufr files
# testoutput : test references
# testinput  : mapping files, Python scripts, and test configuration YAML files
# testrun    : test output files 
# =================================================================================

# Create combined obs types
set(obs_types "")
set(obs_types ${satobs_types})
list(APPEND obs_types ${convobs_types})
list(APPEND obs_types ${convobs_prepbufr_types})

# Create directories and subdirectories
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testinput)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testrun)
# Ensure output directories for each configuration
foreach(test_config ${test_configs})
    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testrun/${test_config})
endforeach()

# Initialize the test_input_files and test_input_configs list
set(test_input_files "")
set(test_input_configs "")

# List for mapping files and Python scripts (test_input_files)
foreach(obs_type ${obs_types}) 
    list(APPEND test_input_files
        "bufr_${obs_type}_mapping.yaml"
        "bufr_${obs_type}.py"
    )
endforeach()

# List for mapping files and Python scripts (test_input_configs) 
foreach (test_config ${test_configs})
    foreach (obs_type ${satobs_types})
        list(APPEND test_input_configs
            "bufr_${test_config}_${obs_type}.yaml"
        )
        foreach (mpi_count ${mpi_counts_satobs})
            if(mpi_count GREATER 0)
                list(APPEND test_input_configs
                    "bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                ) 
	    endif() 		 
        endforeach()
    endforeach()
endforeach()

foreach (test_config ${test_configs})
    foreach (obs_type ${convobs_types})
        list(APPEND test_input_configs
            "bufr_${test_config}_${obs_type}.yaml"
        )
        foreach (mpi_count ${mpi_counts_convobs})
            if(mpi_count GREATER 0)
                list(APPEND test_input_configs
                    "bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                ) 
	    endif() 		 
        endforeach()
    endforeach()
endforeach()

foreach (test_config ${test_configs})
    foreach (obs_type ${convobs_prepbufr_types})
        list(APPEND test_input_configs
            "bufr_${test_config}_${obs_type}.yaml"
        )
        foreach (mpi_count ${mpi_counts_convobs_prepbufr})
            if(mpi_count GREATER 0)
                list(APPEND test_input_configs
                    "bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                )
            endif()
        endforeach()
    endforeach()
endforeach()

# Create symlinks for input mapping files and Python scripts
foreach(file ${test_input_files})
    execute_process(
        COMMAND ${CMAKE_COMMAND} -E create_symlink
        ${CMAKE_CURRENT_SOURCE_DIR}/../sorc/spoc/dump/mapping/${file}
        ${CMAKE_CURRENT_BINARY_DIR}/testinput/${file}
    )
endforeach()

# Create symlinks for input configurations files
foreach(FILENAME ${test_input_configs})
  execute_process( COMMAND ${CMAKE_COMMAND} -E create_symlink
    ${CMAKE_CURRENT_SOURCE_DIR}/testinput/${FILENAME}
    ${CMAKE_CURRENT_BINARY_DIR}/testinput/${FILENAME} )
endforeach(FILENAME)

# ===================================================================
# Download the test data ./build/obsForge/test/ if we don't have it
# ===================================================================

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

# file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_COLLECTION} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

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
