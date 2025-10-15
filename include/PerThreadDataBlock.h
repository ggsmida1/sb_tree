#pragma once
#include <cstdint>
#include <cstddef>
#include "KVPair.h"

// 每线程操作的数据块
class PerThreadDataBlock
{
public:
    // 构造函数
    PerThreadDataBlock();

    // 尾部追加一条 KV；若块已满则返回 false，不修改状态。
    bool Insert(Key key, Value value);

    // 是否已满
    bool IsFull() const;

    // 当前已写入的条目数
    size_t GetNumEntries() const;

    // 只读数据指针(首地址)
    const KVPair *GetData() const;

private:
    // ========================= 常量与容量计算 =========================
    static constexpr size_t kBlockSize = 16384; // 固定块大小（字节）
    // 元数据开销（用于计算可用容量）
    static constexpr size_t kMetadataSize = sizeof(size_t) + sizeof(Key) /*num_entries_ + max_key_*/;
    // 可存放的 KV 条目数
    static constexpr size_t kCapacity = (kBlockSize - kMetadataSize) / sizeof(KVPair);

    size_t num_entries_ = 0; // 已写入的条目数
    KVPair data_[kCapacity]; // 顺序追加的 KV 存储

    // // ========================= 常量与容量计算 =========================

    // // 1. 直接把你的目标容量定义在这里
    // static constexpr size_t kCapacity = 100;

    // // 2. 让其他常量自动根据你的目标容量去计算
    // static constexpr size_t kMetadataSize = sizeof(size_t) + sizeof(Key);
    // static constexpr size_t kBlockSize = kMetadataSize + (kCapacity * sizeof(KVPair));

    // size_t num_entries_ = 0;

    // // 3. 数组自然地使用正确的容量
    // KVPair data_[kCapacity];
};