# test/CMakeLists.txt (最终版)

# -------------------------------------------
# 原有的单线程测试：run_tests
# -------------------------------------------
add_executable(run_tests TestAll.cpp)
target_link_libraries(run_tests PRIVATE sb_tree)
add_test(
    NAME sb_tree_unit_test
    COMMAND run_tests
)

# -------------------------------------------
# 新增的多线程性能测试：run_mt_test
# -------------------------------------------
add_executable(run_mt_test TestMultiThread.cpp)
target_link_libraries(run_mt_test PRIVATE sb_tree)

# 可选：为 CTest 添加多线程测试（如果想在 `ctest` 中统一运行）./test/run_mt_test
add_test(
    NAME sb_tree_mt_test
    COMMAND run_mt_test
)
