# 单线程插入
add_executable(test_single_insert test_single_insert.cpp)
target_link_libraries(test_single_insert PRIVATE sb_tree Threads::Threads)

# 多线程插入
add_executable(test_multi_insert test_multi_insert.cpp)
target_link_libraries(test_multi_insert PRIVATE sb_tree Threads::Threads)

# 插入
add_executable(test_insert test_insert.cpp)
target_link_libraries(test_insert PRIVATE sb_tree Threads::Threads)