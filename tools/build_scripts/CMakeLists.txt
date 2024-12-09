

list(APPEND _scripts
  obsForge_cpp_lint.py
  obsForge_py_lint.sh)

foreach(_f IN LISTS _scripts)
  execute_process( COMMAND ${CMAKE_COMMAND} -E copy
    ${CMAKE_CURRENT_SOURCE_DIR}/${_f} ${CMAKE_BINARY_DIR}/bin/${_f} )
endforeach()
