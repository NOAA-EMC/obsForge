list( APPEND obsforge_provider2ioda_src_files
	obsprovider2ioda.cc
	obsprovider2ioda.h
)

ecbuild_add_executable( TARGET obsforge_obsprovider2ioda.x
                        SOURCES ${obsforge_provider2ioda_src_files} )

target_compile_features( obsforge_obsprovider2ioda.x PUBLIC cxx_std_17)

target_link_libraries( obsforge_obsprovider2ioda.x PUBLIC oops ioda NetCDF::NetCDF_CXX)

link_directories(${CMAKE_SOURCE_DIR}/rtofs)
target_link_libraries(obsforge_obsprovider2ioda.x PRIVATE rtofs)
