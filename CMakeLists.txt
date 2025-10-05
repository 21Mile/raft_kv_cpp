# path: <repo-root>/CMakeLists.txt

cmake_minimum_required(VERSION 3.17)
set(CMAKE_CXX_COMPILER g++) # clang++ / g++ 编译器
project(KVRaftCpp)

# C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Debug build
set(CMAKE_BUILD_TYPE "Debug")

# Output dirs
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH    ${PROJECT_SOURCE_DIR}/lib)
add_definitions(-DPROJECT_ROOT="${CMAKE_SOURCE_DIR}") # 项目根目录常量，供代码里使用
# 保证目录存在（最小修正）
file(MAKE_DIRECTORY ${EXECUTABLE_OUTPUT_PATH})
file(MAKE_DIRECTORY ${LIBRARY_OUTPUT_PATH})

# Avoid PIE text relocation warnings when linking static libs into PIE
set(CMAKE_POSITION_INDEPENDENT_CODE ON) # 统一为所有目标启用 -fPIC

# Threads & Boost.Serialization (for gtest/pthread symbols and boost::archive)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS serialization)
find_package(Protobuf REQUIRED)  # 会导出 protobuf::libprotobuf

# Output dirs（保持原样，虽然上面已设置过一次）
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH    ${PROJECT_SOURCE_DIR}/lib)

# Include dirs（保持原样）
include_directories(${PROJECT_SOURCE_DIR}/src/fiber/include)
include_directories(${PROJECT_SOURCE_DIR}/src/rpc/include)
include_directories(${PROJECT_SOURCE_DIR}/demos)
include_directories(${PROJECT_SOURCE_DIR}/src/raftCore/include)
include_directories(${PROJECT_SOURCE_DIR}/src/raftRpcPro/include)
include_directories(${PROJECT_SOURCE_DIR}/src/thirdparty/include)
include_directories(${PROJECT_SOURCE_DIR}/src/public/include)
include_directories(${PROJECT_SOURCE_DIR}/src/skipList/include)

# Library search dir
link_directories(${PROJECT_SOURCE_DIR}/lib)

# 让 Threads 和 Boost::serialization 对所有目标可用
link_libraries(Threads::Threads Boost::serialization)

# Subdirs
add_subdirectory(src)

# 收集 raftCore / rpc / thirdparty 的所有实现文件
file(GLOB_RECURSE KVRaft_EXTRA_SOURCES CONFIGURE_DEPENDS
  ${PROJECT_SOURCE_DIR}/src/raftCore/*.cc
  ${PROJECT_SOURCE_DIR}/src/raftCore/*.cpp
  ${PROJECT_SOURCE_DIR}/src/rpc/*.cc
  ${PROJECT_SOURCE_DIR}/src/rpc/*.cpp
  ${PROJECT_SOURCE_DIR}/src/thirdparty/*.cc
  ${PROJECT_SOURCE_DIR}/src/thirdparty/*.cpp
)

# 先建库，再把原有变量 + 兜底 GLOB 的源都加进去
add_library(skip_list_on_raft STATIC)
target_sources(skip_list_on_raft
  PRIVATE
    ${src_raftCore} ${src_thirdparty} ${src_public} ${src_raftRpcPro} ${src_fiber}
    ${KVRaft_EXTRA_SOURCES}   # ★ 兜底追加，避免漏编译导致 undefined reference
)

# 导出公共头路径（让下游与库自身编译期都能看到公共头）
target_include_directories(skip_list_on_raft
  PUBLIC
    ${PROJECT_SOURCE_DIR}/src/public/include
    ${PROJECT_SOURCE_DIR}/src/raftRpcPro/include
    ${PROJECT_SOURCE_DIR}/src/rpc/include
    ${PROJECT_SOURCE_DIR}/src/raftCore/include
    ${PROJECT_SOURCE_DIR}/src/thirdparty/include
    ${PROJECT_SOURCE_DIR}/src/skipList/include
)

# 其余保持不变（注意 protobuf 依赖保持 PUBLIC）
target_link_libraries(skip_list_on_raft
  PUBLIC  protobuf::libprotobuf
  PRIVATE muduo_net muduo_base Threads::Threads dl
)

#  将 libTest 提前定义，供 demos 使用（避免目标未定义）
add_library(libTest src/public/m_utils.cc)
target_include_directories(libTest
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src/public/include   # 让使用者自动可见
)

# 再添加 demos（里面若 target_link_libraries(raft_client_run PRIVATE skip_list_on_raft libTest) 即可）
add_subdirectory(demos)

# Tests
enable_testing()
add_subdirectory(tests)
