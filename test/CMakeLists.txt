# test/CMakeLists.txt (修正版)

# 添加一个可执行文件目标，名为 run_tests
# 源文件是当前目录下的 TestAll.cpp
add_executable(run_tests TestAll.cpp)

# 关键步骤：链接我们的测试程序和 SB-Tree 核心库
# 确保这里的库名称与主 CMakeLists.txt 中定义的名称 "sb_tree" 完全一致
target_link_libraries(run_tests PRIVATE sb_tree) # <-- 主要修改在这里

# 将此可执行文件添加为 CTest 的一个测试用例
# 这样我们就可以通过 `ctest` 命令来运行它
add_test(
    NAME sb_tree_unit_test
    COMMAND run_tests
)