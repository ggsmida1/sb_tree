# test/CMakeLists.txt
include(GoogleTest)

# 小函数：1 行拉起一个 gtest 可执行并注册
function(add_sbtest NAME)
  add_executable(${NAME} ${ARGN})
  target_link_libraries(${NAME} PRIVATE sb_tree GTest::gtest_main Threads::Threads)
  gtest_discover_tests(${NAME})
endfunction()

# 现有Google_Test测试
add_sbtest(test_searchlayer_promotion_gtest        test_searchlayer_promotion_gtest.cpp)
add_sbtest(test_lookup_gtest                        test_lookup_gtest.cpp)
add_sbtest(test_tail_unpromoted_gtest               test_tail_unpromoted_gtest.cpp)
add_sbtest(test_end_to_end_gtest                    test_end_to_end_gtest.cpp)
add_sbtest(test_run_seam_and_conversion_gtest       test_run_seam_and_conversion_gtest.cpp)
add_sbtest(test_autodetect_capacity_gtest test_autodetect_capacity_gtest.cpp)
add_sbtest(test_concurrent_insert_gtest test_concurrent_insert_gtest.cpp)

# 两个非-gtest 的可执行（保持原样）
add_executable(test_single_insert test_single_insert.cpp)
target_link_libraries(test_single_insert PRIVATE sb_tree Threads::Threads)

add_executable(test_insert test_insert.cpp)
target_link_libraries(test_insert PRIVATE sb_tree Threads::Threads)
