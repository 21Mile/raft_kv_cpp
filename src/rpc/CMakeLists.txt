get_filename_component(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}" ABSOLUTE)
file(GLOB SRC_LIST "${SRC_DIR}/*.cc")
set(src_rpc ${SRC_LIST} CACHE INTERNAL "Description of the variable")