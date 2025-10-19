# test/CMakeLists.txt

cmake_minimum_required(VERSION 3.12)

# 注意：基本测试已在主CMakeLists.txt中定义，这里只添加额外的测试

# 多线程测试
add_executable(simple_multithread_test simple_multithread_test.cc)
target_link_libraries(simple_multithread_test sb_tree pthread)

add_executable(detailed_multithread_test detailed_multithread_test.cc)
target_link_libraries(detailed_multithread_test sb_tree pthread)

add_executable(multithreaded_insert_test multithreaded_insert_test.cc)
target_link_libraries(multithreaded_insert_test sb_tree pthread)

# 性能对比和演示测试
add_executable(performance_comparison_test performance_comparison_test.cc)
target_link_libraries(performance_comparison_test sb_tree pthread)

add_executable(conservative_performance_test conservative_performance_test.cc)
target_link_libraries(conservative_performance_test sb_tree pthread)

add_executable(final_performance_demo final_performance_demo.cc)
target_link_libraries(final_performance_demo sb_tree pthread)

# 调试和特殊测试
add_executable(debug_insert debug_insert.cc)
target_link_libraries(debug_insert sb_tree pthread)

add_executable(simple_insert_test simple_insert_test.cc)
target_link_libraries(simple_insert_test sb_tree pthread)

add_executable(test_need_conversion test_need_conversion.cc)
target_link_libraries(test_need_conversion sb_tree pthread)

# 纯插入吞吐量测试
add_executable(insert_throughput_test insert_throughput_test.cc)
target_link_libraries(insert_throughput_test sb_tree pthread)

# 简单吞吐量测试（1-2线程）
add_executable(simple_throughput_test simple_throughput_test.cc)
target_link_libraries(simple_throughput_test sb_tree pthread)

add_executable(throughput_modes_test throughput_modes_test.cc)
target_link_libraries(throughput_modes_test sb_tree pthread)

# 安全吞吐量测试（降低插入数量避免崩溃）
add_executable(safe_throughput_test safe_throughput_test.cc)
target_link_libraries(safe_throughput_test sb_tree pthread)

