# test/CMakeLists.txt
include(GoogleTest)

# 小函数：1 行拉起一个 gtest 可执行并注册
function(add_sbtest NAME)
  add_executable(${NAME} ${ARGN})
  target_link_libraries(${NAME} PRIVATE sb_tree GTest::gtest_main Threads::Threads)
  gtest_discover_tests(${NAME})
endfunction()

# 现有Google_Test测试
add_sbtest(test_basic_gtest test_basic_gtest.cpp)
add_sbtest(test_concurrent_gtest test_concurrent_gtest.cpp)
add_sbtest(test_internal_gtest test_internal_gtest.cpp)
add_sbtest(sbtree_single_thread_test sbtree_single_thread_test.cpp)
