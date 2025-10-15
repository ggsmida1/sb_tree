#pragma once
#include <atomic>
#include <cstdint>
#include <mutex>
#include <algorithm>
#include <iterator>
#include <vector>
#include "KVPair.h"
#include "PerThreadDataBlock.h"

enum class BlockStatus : uint8_t
{
    // - ACTIVE :可接受写入；
    // - CONVERT :进入转换流程(不再接受写入,等待收集/合并/切片)
    ACTIVE,
    CONVERT,
};

class SegmentedBlock
{
public:
    // 构造,析构
    SegmentedBlock();
    ~SegmentedBlock();

    // 顺序插入：仅在 ACTIVE 阶段接受写入；否则返回 false。
    bool append_ordered(Key k, Value v);

    // 收集所有已分配 PTB 的数据，合并到一个 vector，并进行全局排序后返回。
    // 说明：仅在封印后调用；返回向量用于上层切片为 DataBlock。
    std::vector<KVPair> collect_and_sort_data();

    // 将状态从 ACTIVE 置为 CONVERT（封印）。封印后不再接受写入。
    void seal();

    // 获取当前块状态（原子读）。
    BlockStatus status() const { return status_.load(std::memory_order_acquire); }

    // 若返回 true，表示需要封印（由“写满”的那次写入置位，上层据此触发切段）。
    bool should_seal() const noexcept { return should_seal_.load(std::memory_order_acquire); }

    // 检查是否完全空 (所有PTB内都没数据)
    bool is_completely_empty() const noexcept;

private:
    // ========================= 内部辅助 =========================
    // 为“当前线程”分配一个专属 PTB 槽位（第一次调用时分配）。
    // 返回槽位下标 [0, kMaxPTBs)，失败返回 -1。
    int get_or_create_slot_for_this_thread_();

    std::atomic<BlockStatus> status_; // 块状态：ACTIVE/CONVERT

    std::atomic<size_t> reserved_count_; // 已保留的 PTB 槽位数（分配阶段）

    // 每线程数据块指针表（按槽位索引）
    static constexpr size_t kMaxPTBs = 128;
    std::atomic<PerThreadDataBlock *> ptb_pointers_[kMaxPTBs];

    // ========================= 封印触发标志 =========================
    // 由“写满”的那次 append_ordered 置位；上层可据此触发切段。
    std::atomic<bool> should_seal_{false};
};