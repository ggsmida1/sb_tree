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

# 多线程测试
add_executable(simple_multithread_test test/simple_multithread_test.cc)
target_link_libraries(simple_multithread_test sb_tree pthread)

add_executable(detailed_multithread_test test/detailed_multithread_test.cc)
target_link_libraries(detailed_multithread_test sb_tree pthread)

add_executable(multithreaded_insert_test test/multithreaded_insert_test.cc)
target_link_libraries(multithreaded_insert_test sb_tree pthread)

# 性能对比和演示测试
add_executable(performance_comparison_test test/performance_comparison_test.cc)
target_link_libraries(performance_comparison_test sb_tree pthread)

add_executable(conservative_performance_test test/conservative_performance_test.cc)
target_link_libraries(conservative_performance_test sb_tree pthread)

add_executable(final_performance_demo test/final_performance_demo.cc)
target_link_libraries(final_performance_demo sb_tree pthread)

# 调试和特殊测试
add_executable(debug_insert test/debug_insert.cc)
target_link_libraries(debug_insert sb_tree pthread)

add_executable(simple_insert_test test/simple_insert_test.cc)
target_link_libraries(simple_insert_test sb_tree pthread)

add_executable(test_need_conversion test/test_need_conversion.cc)
target_link_libraries(test_need_conversion sb_tree pthread)

# 纯插入吞吐量测试
add_executable(insert_throughput_test test/insert_throughput_test.cc)
target_link_libraries(insert_throughput_test sb_tree pthread)

# 简单吞吐量测试（1-2线程）
add_executable(simple_throughput_test test/simple_throughput_test.cc)
target_link_libraries(simple_throughput_test sb_tree pthread)

add_executable(throughput_modes_test test/throughput_modes_test.cc)
target_link_libraries(throughput_modes_test sb_tree pthread)

# 安全吞吐量测试（降低插入数量避免崩溃）
add_executable(safe_throughput_test test/safe_throughput_test.cc)
target_link_libraries(safe_throughput_test sb_tree pthread)

# 包含测试目录的CMakeLists.txt（如果需要额外测试）
# add_subdirectory(test)

# 安装目标
install(TARGETS sb_tree basic_test performance_test concurrent_test benchmark
        simple_multithread_test detailed_multithread_test multithreaded_insert_test
        performance_comparison_test conservative_performance_test final_performance_demo
        debug_insert simple_insert_test test_need_conversion
        insert_throughput_test simple_throughput_test throughput_modes_test safe_throughput_test
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