#pragma once
#include <cstdint>
#include <cstddef>
#include <vector>
#include <limits>
#include <utility>
#include "KVPair.h"

// 数据块（4KB 定长，包含 N-ary 索引 + KV 分离存储）
class DataBlock
{
public:
    // --- 状态枚举 ---
    enum class Status : uint8_t
    {
        READY,
        SPLITTING
    };

    DataBlock();

    // 由“已排序”的 KV 构建；返回写入条数（<= kCapacity）
    size_t build_from_sorted(const KVPair *src, size_t n);

    // 查找：先 N-ary 桶定位，再桶内线扫
    bool find(Key k, Value &out) const;

    // 扫描：从 startKey 起，最多取 count 个，写入 out；返回实际条数
    size_t scan_from(Key startKey, size_t count, std::vector<Value> &out) const;

    // 访问器
    size_t size() const { return count_; }
    Key min_key() const { return min_key_; }
    DataBlock *next() const { return next_; }
    void set_next(DataBlock *p) { next_ = p; }

private:
    // === 固定大小常量 ===
    static constexpr size_t kBlockSize = 4096; // 整块大小 4KB
    static constexpr size_t kBuckets = 8;      // N-ary 桶数（固定）
    using LockWord = uint32_t;                 // 轻量“锁位”

    // Header 占用字节数
    static constexpr size_t kHeaderSize =
        sizeof(Status) + sizeof(Key) + sizeof(void *) +
        sizeof(LockWord) + sizeof(uint32_t);

    // N-ary 表占用
    static constexpr size_t kNarySize = kBuckets * sizeof(Key);

    // 留给 KV 的空间
    static constexpr size_t kKVBytes =
        (kBlockSize >= kHeaderSize + kNarySize)
            ? (kBlockSize - kHeaderSize - kNarySize)
            : 0;

    static constexpr size_t kOneEntryBytes = sizeof(Key) + sizeof(Value);
    static constexpr size_t kCapacity = kKVBytes / kOneEntryBytes;

    static_assert(kCapacity > 0, "DataBlock capacity must be > 0 under 4KB.");

    // 内部辅助函数
    void build_nary_();
    std::pair<size_t, size_t> bucket_range_(Key k) const;

    // === 元数据字段 ===
    Status status_;   // 状态
    Key min_key_;     // 块内最小 key
    DataBlock *next_; // 下一个数据块
    LockWord lock_;   // 轻量锁
    uint32_t count_;  // 实际条目数

    // === 数据区 ===
    Key nary_[kBuckets];    // N-ary表
    Key keys_[kCapacity];   // Key数组
    Value vals_[kCapacity]; // Value数组
};
