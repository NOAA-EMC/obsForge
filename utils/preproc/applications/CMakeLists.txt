list( APPEND obsforge_provider2ioda_src_files
	obsprovider2ioda.cc
	obsprovider2ioda.h
)

ecbuild_add_executable( TARGET obsforge_obsprovider2ioda.x
                        SOURCES ${obsforge_provider2ioda_src_files} )

target_compile_features( obsforge_obsprovider2ioda.x PUBLIC cxx_std_17)

target_link_libraries( obsforge_obsprovider2ioda.x PUBLIC oops ioda netcdf2ioda NetCDF::NetCDF_CXX)

# Set the RPATH to the build directory
set_target_properties(obsforge_obsprovider2ioda.x PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "${CMAKE_BINARY_DIR}/lib"
    INSTALL_RPATH_USE_LINK_PATH TRUE
)
