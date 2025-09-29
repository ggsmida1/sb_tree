#pragma once
#include <cstdint>
#include <cstddef>
#include <vector>
#include <limits>
#include <utility>
#include "KVPair.h"

// -----------------------------------------------------------------------------
// DataBlock
// -----------------------------------------------------------------------------
// 作用：
//   - 数据层的叶子块（通常 4KB 定长）。
//   - 采用 Key/Value 分离存储，并维持块内有序，便于查找与顺序扫描。
//   - 使用 N-ary 搜索表先粗定位到“桶”，再在桶内进行短线性扫描。
// 并发语义：
//   - DataBlock 一旦构建完成即不可变（immutable）；
//   - 并发写入通过 PerThreadBlock 和 SegmentedBlock 完成，
//     以追加新 DataBlock 的方式表现，不会修改已有 DataBlock。
// 不变式：
//   - keys_[0..count_-1] 严格非降序；
//   - vals_ 与 keys_ 下标一一对应；
//   - nary_[i] 为第 i 个桶的最小 key，单调不降；
//   - next_ 形成叶子链表（按 key 递增）。
// -----------------------------------------------------------------------------
class DataBlock
{
public:
    // ========================= 公共接口（对外可见） =========================
    // --- 状态枚举：标记块是否可读或处于分裂中 ---
    enum class Status : uint8_t
    {
        READY,
        SPLITTING
    };

    // --- 构造与构建 ---
    DataBlock(); // 默认构造：初始化元数据
    // 从“已排序”的 KV 数组构建本块；返回实际写入条数（<= kCapacity）。
    size_t build_from_sorted(const KVPair *src, size_t n);

    // --- 点查（Point Lookup） ---
    // 先用 N-ary 表定位桶，再在桶内线性扫描；命中返回 true 并写 out。
    bool find(Key k, Value &out) const;

    // --- 扫描（Scan） ---
    // 从 startKey（含）起，最多取 count 个，结果追加到 out；返回实际条数。
    size_t scan_from(Key startKey, size_t count, std::vector<Value> &out) const;
    // 在 [start, end]（闭区间）范围内扫描，将命中 value 追加到 out；返回条数。
    size_t scan_range(Key start, Key end, std::vector<Value> &out) const;

    // --- 访问器与链表链接 ---
    size_t size() const { return count_; }     // 当前条目数
    Key min_key() const { return min_key_; }   // 本块最小 key
    DataBlock *next() const { return next_; }  // 后继数据块
    void set_next(DataBlock *p) { next_ = p; } // 设置后继数据块

    // --- 测试辅助（可选） ---
    // 直接按索引读取键值（无边界检查；测试/校验用）。
    KVPair get_entry(size_t index) const { return {keys_[index], vals_[index]}; }

private:
    // ========================= 常量与布局（仅内部） =========================
    static constexpr size_t kBlockSize = 4096; // 整块大小：4KB
    static constexpr size_t kBuckets = 8;      // N-ary 桶数（固定）
    using LockWord = uint32_t;                 // 轻量锁位（预留）

    // 头部开销（仅用于估算容量，不要求紧凑内存布局）
    static constexpr size_t kHeaderSize =
        sizeof(Status) + sizeof(Key) + sizeof(void *) +
        sizeof(LockWord) + sizeof(uint32_t);

    // N-ary 表占用字节数
    static constexpr size_t kNarySize = kBuckets * sizeof(Key);

    // 留给 KV 的有效字节数
    static constexpr size_t kKVBytes =
        (kBlockSize >= kHeaderSize + kNarySize)
            ? (kBlockSize - kHeaderSize - kNarySize)
            : 0;

    // 单条 KV 所需字节数；据此估算块容量
    static constexpr size_t kOneEntryBytes = sizeof(Key) + sizeof(Value);
    static constexpr size_t kCapacity = kKVBytes / kOneEntryBytes;
    static_assert(kCapacity > 0, "DataBlock capacity must be > 0 under 4KB.");

    // ========================= 内部辅助函数 =========================
    void build_nary_();                                   // 依据 keys_ 构建 N-ary 表
    std::pair<size_t, size_t> bucket_range_(Key k) const; // 计算 key 所在桶的 [l,r)

    // ========================= 元数据字段 =========================
    Status status_ = Status::READY;                 // 块状态（预留）
    Key min_key_ = std::numeric_limits<Key>::max(); // 块内最小 key
    DataBlock *next_ = nullptr;                     // 指向后继 DataBlock
    LockWord lock_ = 0;                             // 轻量锁（预留）
    uint32_t count_ = 0;                            // 实际填充条目数

    // ========================= 数据区 =========================
    Key nary_[kBuckets] = {};    // N-ary 搜索表（每桶的最小 key）
    Key keys_[kCapacity] = {};   // Key 数组（有序）
    Value vals_[kCapacity] = {}; // Value 数组（与 keys_ 对齐）
};
