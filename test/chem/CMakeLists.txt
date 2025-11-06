# create runtime directory for chem tests
file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/chem/run)

ecbuild_add_test( 
  TARGET test_obsforge_global_aod_dump
  TYPE SCRIPT
  COMMAND ${PROJECT_SOURCE_DIR}/chem/run_test_obsforge_global_aod_dump.sh
  ARGS ${PROJECT_BINARY_DIR} ${PROJECT_SOURCE_DIR}
  WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/chem/run
  TEST_DEPENDS test_obsforge_staging_gcdas
)
