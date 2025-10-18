cmake_minimum_required(VERSION 3.10)
project(SBTree)

# 设置C++标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 设置编译选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O3")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")

# 包含目录
include_directories(include)

# 源文件
set(SB_TREE_SOURCES
    src/nary_search_table.cc
    src/data_block.cc
    src/per_thread_data_block.cc
    src/search_node.cc
    src/block_allocator.cc
    src/segmented_block.cc
    src/segmented_block_converter.cc
    src/sb_tree.cc
)

# 创建静态库
add_library(sb_tree STATIC ${SB_TREE_SOURCES})

# 设置包含目录
target_include_directories(sb_tree PUBLIC include)

# 链接线程库
find_package(Threads REQUIRED)
target_link_libraries(sb_tree Threads::Threads)

# 基本功能测试
add_executable(basic_test test/basic_test.cc)
target_link_libraries(basic_test sb_tree)

# 性能测试
add_executable(performance_test test/performance_test.cc)
target_link_libraries(performance_test sb_tree)

# 并发测试
add_executable(concurrent_test test/concurrent_test.cc)
target_link_libraries(concurrent_test sb_tree)

# 基准测试
add_executable(benchmark test/benchmark_test.cc)
target_link_libraries(benchmark sb_tree)

# 调试测试
add_executable(debug_test test/debug_test.cc)
target_link_libraries(debug_test sb_tree)

# 简单测试
add_executable(simple_test test/simple_test.cc)
target_link_libraries(simple_test sb_tree)

# 安装目标
install(TARGETS sb_tree basic_test performance_test concurrent_test benchmark
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

# 安装头文件
install(DIRECTORY include/ DESTINATION include)

# 测试目标
enable_testing()

# 添加测试
add_test(NAME basic_test COMMAND basic_test)
add_test(NAME performance_test COMMAND performance_test)
add_test(NAME concurrent_test COMMAND concurrent_test)

# 设置测试属性
set_tests_properties(basic_test PROPERTIES TIMEOUT 60)
set_tests_properties(performance_test PROPERTIES TIMEOUT 300)
set_tests_properties(concurrent_test PROPERTIES TIMEOUT 300)

# 打印配置信息
message(STATUS "SB-Tree Configuration:")
message(STATUS "  C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ Flags: ${CMAKE_CXX_FLAGS}")
message(STATUS "  Install Prefix: ${CMAKE_INSTALL_PREFIX}")