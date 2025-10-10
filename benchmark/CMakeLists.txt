# 设置最低 CMake 版本要求
cmake_minimum_required(VERSION 3.10)

# 添加可执行文件目标
add_executable(sbtree_benchmark benchmark_sbtree.cpp)

# 引入 Google Benchmark
# 推荐使用 FetchContent 在构建时自动下载和配置
include(FetchContent)
FetchContent_Declare(
  benchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG        v1.8.0
)
FetchContent_MakeAvailable(benchmark)

# 链接 SB-Tree 的核心库
# 这里的 "sb_tree" 必须和你主 CMakeLists.txt 文件里 add_library 定义的名字完全一致
target_link_libraries(sbtree_benchmark PRIVATE
    sb_tree         # 链接你的 SB-Tree 库 (已更正)
    benchmark::benchmark # 链接 Google Benchmark
    Threads::Threads     # 链接线程库 (更现代的写法)
)

# C++ 标准
target_compile_features(sbtree_benchmark PRIVATE cxx_std_17)

