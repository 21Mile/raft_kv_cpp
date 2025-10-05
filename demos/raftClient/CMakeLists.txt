# path: demos/raftClient/CMakeLists.txt

get_filename_component(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}" ABSOLUTE)
aux_source_directory(${SRC_DIR} SRC_LIST)

add_executable(raft_client_run ${SRC_LIST})

# 头文件路径：本目录 + 项目公共头 + 生成的 pb 头所在目录
target_include_directories(raft_client_run PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/public/include
    ${PROJECT_SOURCE_DIR}/src/raftRpcPro/include
    ${PROJECT_SOURCE_DIR}/src/rpc/include
)

# 链接库：核心库 + 公共工具库 + protobuf + 线程
target_link_libraries(raft_client_run PRIVATE
    skip_list_on_raft
    libTest
    protobuf::libprotobuf
    Threads::Threads
    pthread
)
