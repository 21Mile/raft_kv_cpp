include(CTest)

# set test build target name
set(TEST_BINARY ${PROJECT_NAME}_test)

# add new test
add_executable(${TEST_BINARY} test_util.cc)

# 指定测试的名称
add_test(test_1 tests/test_util.cc)

# link libraries to use in this executable
# link gtest_main to avoid your own main.cpp
target_link_libraries(${TEST_BINARY} PUBLIC libTest gtest gtest_main)