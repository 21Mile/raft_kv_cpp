get_filename_component(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}" ABSOLUTE)
file(GLOB SRC_LIST "${SRC_DIR}/*.cc")
# file(GLOB SRC_LIST "${SRC_DIR}/*.hpp")
set(src_thirdparty ${SRC_LIST} CACHE INTERNAL "Description of the variable")