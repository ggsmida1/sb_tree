cmake_minimum_required(VERSION 3.14)
project(sb_tree LANGUAGES CXX)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_compile_options(-g -O1 -fsanitize=address -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 库
add_library(sb_tree
    src/PerThreadDataBlock.cpp
    src/SegmentedBlock.cpp
    src/DataBlock.cpp
    src/SBTree.cpp
    src/SearchLayer.cpp
)
target_include_directories(sb_tree PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

find_package(Threads REQUIRED)

# GoogleTest（顶层拉取一次）
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
set(gtest_force_shared_crt OFF CACHE BOOL "Use shared CRT" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()
add_subdirectory(test)
