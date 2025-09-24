cmake_minimum_required(VERSION 3.12)
project(SBTree LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 库：src/sb-tree.cpp
add_library(sb-tree STATIC
    src/sb-tree.cpp
)
target_include_directories(sb-tree PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# 线程库（跨平台）
find_package(Threads REQUIRED)

# 测试子目录
add_subdirectory(test)
