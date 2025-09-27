#pragma once
#include <cstdint>
#include <cstddef>
#include "KVPair.h"

// 每线程数据块（4KB 定长，顺序追加）
// 注意：不做并发同步，外部按“每线程独占”使用
class PerThreadDataBlock
{
public:
    PerThreadDataBlock();

    // 追加一条；满了返回 false
    bool Insert(Key key, Value value);

    // 是否已满
    bool IsFull() const;

    // —— 转换阶段需要的只读接口 ——
    size_t GetNumEntries() const;
    const KVPair *GetData() const;

private:
    // 固定块大小
    static constexpr size_t kBlockSize = 16384;

    // 元数据
    size_t num_entries_;
    Key max_key_;

    // 容量计算：剔除元数据占用后的可存放条数
    static constexpr size_t kMetadataSize = sizeof(num_entries_) + sizeof(max_key_);
    static constexpr size_t kCapacity = (kBlockSize - kMetadataSize) / sizeof(KVPair);

    // 实际数据
    KVPair data_[kCapacity];
};
