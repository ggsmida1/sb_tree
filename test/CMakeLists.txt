# test/CMakeLists.txt
cmake_minimum_required(VERSION 3.12)

# -----------------------------
# 基础测试：功能正确性验证
# -----------------------------
add_executable(run_basic_test TestInsertBasic.cpp)
target_link_libraries(run_basic_test PRIVATE sb_tree)
add_test(NAME sb_tree_basic_test COMMAND run_basic_test)

# -----------------------------
# 单线程测试（原有）
# -----------------------------
add_executable(run_tests TestAll.cpp)
target_link_libraries(run_tests PRIVATE sb_tree)
add_test(NAME sb_tree_unit_test COMMAND run_tests)

# -----------------------------
# 延迟数据测试（新增）
# -----------------------------
add_executable(run_delayed_test TestDelayedData.cpp)
target_link_libraries(run_delayed_test PRIVATE sb_tree)
add_test(NAME sb_tree_delayed_test COMMAND run_delayed_test)

# -----------------------------
# 多线程性能测试（原有）
# -----------------------------
add_executable(run_mt_test TestMultiThread.cpp)
target_link_libraries(run_mt_test PRIVATE sb_tree)
add_test(NAME sb_tree_mt_test COMMAND run_mt_test)
add_executable(run_benchmark TestInsertBenchmark.cpp)
target_link_libraries(run_benchmark PRIVATE sb_tree)
add_test(NAME sb_tree_benchmark COMMAND run_benchmark)

# -----------------------------
# 功能性回归测试（新增）
# -----------------------------
add_executable(run_functional TestFunctional.cpp)
target_link_libraries(run_functional PRIVATE sb_tree)
add_test(NAME sb_tree_functional COMMAND run_functional)