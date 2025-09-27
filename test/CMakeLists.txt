include(GoogleTest)

# GTest：晋升规则
add_executable(test_searchlayer_promotion_gtest test_searchlayer_promotion_gtest.cpp)
target_link_libraries(test_searchlayer_promotion_gtest
  PRIVATE sb_tree GTest::gtest_main Threads::Threads)
gtest_discover_tests(test_searchlayer_promotion_gtest)

# GTest：端到端 lookup
add_executable(test_lookup_gtest test_lookup_gtest.cpp)
target_link_libraries(test_lookup_gtest
  PRIVATE sb_tree GTest::gtest_main Threads::Threads)
gtest_discover_tests(test_lookup_gtest)

add_executable(test_tail_unpromoted_gtest
  test_tail_unpromoted_gtest.cpp
)
target_link_libraries(test_tail_unpromoted_gtest
  PRIVATE sb_tree GTest::gtest_main Threads::Threads
)
gtest_discover_tests(test_tail_unpromoted_gtest)

add_executable(test_end_to_end_gtest
  test_end_to_end_gtest.cpp
)
target_link_libraries(test_end_to_end_gtest
  PRIVATE sb_tree GTest::gtest_main Threads::Threads
)
gtest_discover_tests(test_end_to_end_gtest)



# 你原来的两个测试
add_executable(test_single_insert test_single_insert.cpp)
target_link_libraries(test_single_insert PRIVATE sb_tree Threads::Threads)

add_executable(test_insert test_insert.cpp)
target_link_libraries(test_insert PRIVATE sb_tree Threads::Threads)


