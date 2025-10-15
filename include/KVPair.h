#pragma once
#include <cstdint>

using Key = uint64_t;   // Key 类型，当前为 64 位无符号整数
using Value = uint64_t; // Value 类型，当前为 64 位无符号整数

struct KVPair
{
    Key key;     // 键
    Value value; // 值
};