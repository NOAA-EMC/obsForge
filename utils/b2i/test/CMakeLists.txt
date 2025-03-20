# Find Python (required for running tests)
find_package(Python REQUIRED)
string(REGEX REPLACE "^([0-9]+\\.[0-9]+).*" "\\1" PYTHON_MAJOR_MINOR ${Python_VERSION})
set(PYIODACONV_DIR "${CMAKE_BINARY_DIR}/lib/python${PYTHON_MAJOR_MINOR}/")


# Define test input YAML files
list(APPEND b2i_test_input
    testinput/bufr2ioda_insitu_profile_argo_2021063006.yaml.in
    testinput/bufr2ioda_insitu_profile_bathy_2021063006.yaml.in
    testinput/bufr2ioda_insitu_profile_glider_2021063006.yaml.in
    testinput/bufr2ioda_insitu_profile_tesac_2021063006.yaml.in
    testinput/bufr2ioda_insitu_profile_tropical_2019010700.yaml.in
    testinput/bufr2ioda_insitu_profile_xbtctd_2021063006.yaml.in
    testinput/bufr2ioda_insitu_surface_drifter_2019010700.yaml.in
    testinput/bufr2ioda_insitu_surface_trkob_2021063006.yaml.in
)

# Define reference output files
list(APPEND b2i_test_ref
    testref/bufr2ioda_insitu_profile_argo_2021063006.ref
    testref/bufr2ioda_insitu_profile_bathy_2021063006.ref
    testref/bufr2ioda_insitu_profile_glider_2021063006.ref
    testref/bufr2ioda_insitu_profile_tesac_2021063006.ref
    testref/bufr2ioda_insitu_profile_tropical_2019010700.ref
    testref/bufr2ioda_insitu_profile_xbtctd_2021063006.ref
    testref/bufr2ioda_insitu_surface_drifter_2019010700.ref
    testref/bufr2ioda_insitu_surface_trkob_2021063006.ref
)

# Set up b2i-specific test directories
set(B2I_TEST_DIR ${CMAKE_CURRENT_BINARY_DIR}/b2i/test)
set(B2I_TESTINPUT_DIR ${B2I_TEST_DIR}/testinput)
set(B2I_TESTREF_DIR ${B2I_TEST_DIR}/testref)
set(B2I_TESTOUTPUT_DIR ${B2I_TEST_DIR}/testoutput)

file(MAKE_DIRECTORY ${B2I_TESTINPUT_DIR})
file(MAKE_DIRECTORY ${B2I_TESTREF_DIR})
file(MAKE_DIRECTORY ${B2I_TESTOUTPUT_DIR})

# Symlink test input and reference files
foreach(FILENAME ${b2i_test_input})
    get_filename_component(filename ${FILENAME} NAME)
    file(CREATE_LINK ${CMAKE_CURRENT_SOURCE_DIR}/${FILENAME}
         ${B2I_TESTINPUT_DIR}/${filename}
         SYMBOLIC)
endforeach()

foreach(FILENAME ${b2i_test_ref})
    get_filename_component(filename ${FILENAME} NAME)
    file(CREATE_LINK ${CMAKE_CURRENT_SOURCE_DIR}/${FILENAME}
         ${B2I_TESTREF_DIR}/${filename}
         SYMBOLIC)
endforeach()

# Define paths for BUFR-to-IODA scripts and executable
# careful: this assume the test dir is a subdir of the script dir
set(B2I_SCRIPT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/..")

# Function to process .in files
function(CREATE_CONFIG_FILE 
	test_config_in 
	test_config_out 
	bufr_input_dir 
	ioda_output_dir 
	ocean_basin_file
)
    file(READ "${test_config_in}" file_content)
    string(REPLACE "__BUFRINPUTDIR__" "\"${bufr_input_dir}\"" temp_content "${file_content}")
    string(REPLACE "__IODAOUTPUTDIR__" "\"${ioda_output_dir}\"" temp_content2 "${temp_content}")
    string(REPLACE "__OCEANBASIN__" "\"${ocean_basin_file}\"" temp_content3 "${temp_content2}")
    file(WRITE "${test_config_out}" "${temp_content3}")
endfunction()

# Function to check paths
function(CHECK_AND_SET_PATH PATH1 PATH2 RESULT_VAR)
    if(EXISTS ${PATH1})
        set(${RESULT_VAR} ${PATH1} PARENT_SCOPE)
        set(${RESULT_VAR}_EXISTS TRUE PARENT_SCOPE)
    elseif(EXISTS ${PATH2})
        set(${RESULT_VAR} ${PATH2} PARENT_SCOPE)
        set(${RESULT_VAR}_EXISTS TRUE PARENT_SCOPE)
    else()
        set(${RESULT_VAR} "" PARENT_SCOPE)
        set(${RESULT_VAR}_EXISTS FALSE PARENT_SCOPE)
    endif()
endfunction()

# Locate BUFR input and ocean basin files
set(BUFR_TEST_DIR_ORION "/work/noaa/da/marineda/gfs-marine/data/obs/ci/bufr")
set(BUFR_TEST_DIR_HERA "/scratch1/NCEPDEV/da/common/ci/bufr")
CHECK_AND_SET_PATH(${BUFR_TEST_DIR_ORION} ${BUFR_TEST_DIR_HERA} BUFR_TEST_DIR)

set(OCEAN_BASIN_FILE_ORION "/work/noaa/global/glopara/fix/gdas/soca/20240802/common/RECCAP2_region_masks_all_v20221025.nc")
set(OCEAN_BASIN_FILE_HERA "/scratch2/NCEPDEV/ocean/Guillaume.Vernieres/data/static/common/RECCAP2_region_masks_all_v20221025.nc")
CHECK_AND_SET_PATH(${OCEAN_BASIN_FILE_ORION} ${OCEAN_BASIN_FILE_HERA} OCEAN_BASIN_FILE)

if(NOT BUFR_TEST_DIR_EXISTS OR NOT OCEAN_BASIN_FILE_EXISTS)
    message(WARNING "Required BUFR or ocean basin files not found; skipping b2i tests.")
    set(GENERATE_B2I_TESTS FALSE)
else()
    set(GENERATE_B2I_TESTS TRUE)
endif()

# Function to add b2i tests
function(ADD_B2I_TEST testname testbufr)
	# set(CONFIG_TYPE "json")
    set(CONFIG_TYPE "yaml")
    if(testbufr STREQUAL "dbuoy")
        set(DATE "2019010700")
        set(CYCLE "00")
    else()
        set(DATE "2021063006")
        set(CYCLE "06")
    endif()

    set(TEST "bufr2ioda_insitu_${testname}")
    set(TESTREF_FILE "${TEST}_${DATE}.ref")
    set(BUFR_INPUT_DIR ${B2I_TEST_DIR})
    set(IODA_OUTPUT_DIR ${B2I_TESTOUTPUT_DIR})
    set(BUFR_TEST_FILE "${DATE}-gdas.t${CYCLE}z.${testbufr}.tm00.bufr_d")
    set(BUFR_FILE "${BUFR_TEST_DIR}/${BUFR_TEST_FILE}")

    if(NOT EXISTS ${BUFR_FILE})
        message(WARNING "BUFR file ${BUFR_FILE} not found; skipping test ${TEST}")
        return()
    endif()
    file(COPY ${BUFR_FILE} DESTINATION ${BUFR_INPUT_DIR})

    set(CONFIG_FILE_NAME "${TEST}_${DATE}.${CONFIG_TYPE}")
    set(CONFIG_FILE_IN "${B2I_TESTINPUT_DIR}/${TEST}_${DATE}.${CONFIG_TYPE}.in")
    set(CONFIG_FILE "${B2I_TEST_DIR}/${CONFIG_FILE_NAME}")
    CREATE_CONFIG_FILE(
		${CONFIG_FILE_IN} 
		${CONFIG_FILE} 
		${BUFR_INPUT_DIR} 
		${IODA_OUTPUT_DIR} 
		${OCEAN_BASIN_FILE}
	)

    add_test(NAME test_b2i_${TEST}
             COMMAND ${B2I_SCRIPT_DIR}/${TEST}.py -c ${CONFIG_FILE} -t ${B2I_TESTREF_DIR}/${TESTREF_FILE}
             WORKING_DIRECTORY ${B2I_TEST_DIR})
    set_property(TEST test_b2i_${TEST}
                 APPEND PROPERTY 
					ENVIRONMENT "PYTHONPATH=${PYIODACONV_DIR}:$ENV{PYTHONPATH}")
endfunction()

# Add tests if prerequisites are met
if(GENERATE_B2I_TESTS)
    ADD_B2I_TEST("profile_argo" "subpfl")
    ADD_B2I_TEST("profile_bathy" "bathy")
    ADD_B2I_TEST("profile_glider" "subpfl")
    ADD_B2I_TEST("profile_tesac" "tesac")
    ADD_B2I_TEST("profile_tropical" "dbuoy")
    ADD_B2I_TEST("profile_xbtctd" "xbtctd")
    ADD_B2I_TEST("surface_drifter" "dbuoy")
    ADD_B2I_TEST("surface_trkob" "trkob")
endif()
