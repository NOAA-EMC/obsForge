# create runtime directory for atmos tests
file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/atmos/run)

ecbuild_add_test( 
  TARGET test_obsforge_global_atmos_bufr_dump
  TYPE SCRIPT
  COMMAND ${PROJECT_SOURCE_DIR}/atmos/run_test_obsforge_global_atmos_bufr_dump.sh
  ARGS ${PROJECT_BINARY_DIR} ${PROJECT_SOURCE_DIR}
  WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/atmos/run
  TEST_DEPENDS test_obsforge_staging_gdas
)
