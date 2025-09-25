cmake_minimum_required(VERSION 3.14)
project(sb_tree LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 先建库
add_library(sb_tree
    src/PerThreadDataBlock.cpp
    src/SegmentedBlock.cpp
    src/DataBlock.cpp
    src/SBTree.cpp
)

# 再加 include 路径
target_include_directories(sb_tree PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

# 找线程库
find_package(Threads REQUIRED)

# 加载 test 子目录
add_subdirectory(test)
