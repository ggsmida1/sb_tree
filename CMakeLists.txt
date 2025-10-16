cmake_minimum_required(VERSION 3.14)
project(sb_tree LANGUAGES CXX)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_compile_options(-g -O1 -fsanitize=address -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 定义 sb_tree 核心库
add_library(sb_tree
    src/PerThreadDataBlock.cpp
    # --- 主要修改：移除了下面这一行，因为它已经被删除了 ---
    # src/SegmentedBlock.cpp 
    src/DataBlock.cpp
    src/SBTree.cpp
    src/SearchLayer.cpp
)
# 为 sb_tree 库及其使用者（PUBLIC）提供头文件路径
target_include_directories(sb_tree PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

# --- 后续内容保持不变 ---
find_package(Threads REQUIRED)

include(FetchContent)

FetchContent_Declare(
  googletest
  URL "https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip"
)

FetchContent_Declare(
  benchmark
  GIT_REPOSITORY "https://github.com/google/benchmark.git"
  GIT_TAG        "v1.8.0"
)

set(gtest_force_shared_crt OFF CACHE BOOL "Use shared CRT" FORCE)
FetchContent_MakeAvailable(googletest benchmark)

enable_testing()
add_subdirectory(test)
# add_subdirectory(benchmark)