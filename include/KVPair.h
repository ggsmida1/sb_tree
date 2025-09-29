#pragma once
#include <cstdint>

// ============================
// KVPair.h — Key/Value 基本定义
// ============================
// 作用：
//   - 提供统一的 Key 和 Value 类型定义；
//   - 定义 KVPair 结构体，表示一条键值对；
//   - 在整个 SB-Tree 中作为最小的数据单元传递。
// 注意：
//   - 当前 Key 和 Value 都是 uint64_t，可在未来根据需要修改。
// ============================

using Key = uint64_t;   // Key 类型，当前为 64 位无符号整数
using Value = uint64_t; // Value 类型，当前为 64 位无符号整数

struct KVPair
{
    Key key;     // 键
    Value value; // 值
};