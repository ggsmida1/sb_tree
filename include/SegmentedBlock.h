#pragma once
#include <atomic>
#include <cstdint>
#include <mutex>
#include <algorithm>
#include <iterator>
#include <vector>
#include "KVPair.h"
#include "PerThreadDataBlock.h"

// -----------------------------------------------------------------------------
// BlockStatus
// -----------------------------------------------------------------------------
// 作用：标识分段块（SegmentedBlock）的当前生命周期阶段。
// - ACTIVE ：可接受写入；
// - CONVERT ：进入转换流程（不再接受写入，等待收集/合并/切片）；
// - CONVERTED：转换完成（交由上层追加到数据层链表）。
// -----------------------------------------------------------------------------
enum class BlockStatus : uint8_t
{
    ACTIVE,
    CONVERT,
    CONVERTED,
};
// -----------------------------------------------------------------------------
// SegmentedBlock
// -----------------------------------------------------------------------------
// 作用：
// - 管理多线程各自的 PerThreadDataBlock（PTB），承接热写入；
// - 当任一 PTB 写满或上层策略触发时，封印本段并进入转换；
// - 转换阶段负责“收集 → 归并排序 → 产出有序 KV 向量”，供上层切片为 DataBlock；
// 并发语义：
// - 追加写入遵循“每线程独占其 PTB 槽位”的设计；
// - 封印（seal）后不再接受写入；
// - 状态使用原子变量，支持多线程并发读写状态标志；
// 不变式（约定）：
// - ACTIVE 阶段允许 append_ordered；CONVERT/CONVERTED 阶段拒绝写入；
// - min_key_ 记录该段内观测到的最小 key（用于上层构建叶层有序性断言/优化）；
// - ptb_pointers_ 中已分配槽位仅由对应线程使用；
// 注意：
// - 本类不直接产出 DataBlock；只负责“汇聚成有序向量”，切片由上层完成。
// -----------------------------------------------------------------------------
class SegmentedBlock
{
public:
    // ========================= 构造/析构 =========================
    SegmentedBlock();
    ~SegmentedBlock();

    // ========================= 写入接口 =========================
    // 顺序插入：仅在 ACTIVE 阶段接受写入；否则返回 false。
    // 约定：在单调递增工作负载下，可用作轻量断言与范围估计（更新 min_key_ 等）。
    bool append_ordered(Key k, Value v);

    // ========================= 收集与排序 =========================
    // 收集所有已分配 PTB 的数据，合并到一个 vector，并进行全局排序后返回。
    // 说明：仅在封印后调用；返回向量用于上层切片为 DataBlock。
    std::vector<KVPair> collect_and_sort_data();

    // ========================= 状态管理 =========================
    // 将状态从 ACTIVE 置为 CONVERT（幂等）。封印后不再接受写入。
    void seal();

    // 获取当前块状态（原子读）。
    BlockStatus status() const { return status_.load(std::memory_order_acquire); }

    // 若返回 true，表示需要封印（由“写满”的那次写入置位，上层据此触发切段）。
    bool should_seal() const noexcept { return should_seal_.load(std::memory_order_acquire); }

private:
    // ========================= 内部辅助 =========================
    // 为“当前线程”分配一个专属 PTB 槽位（第一次调用时分配）。
    // 返回槽位下标 [0, kMaxPTBs)，失败返回 -1。
    int get_or_create_slot_for_this_thread_();

    // ========================= 元数据与状态 =========================
    std::atomic<BlockStatus> status_; // 块状态：ACTIVE/CONVERT/CONVERTED
    std::atomic<Key> min_key_;        // 本段观测到的最小 key（用于顺序性/断言）
    SegmentedBlock *next_block_;      // 指向数据层的后继段（由上层维护）
    std::mutex lock_;                 // 转换阶段的互斥（保护收集/排序等临界区）

    // ========================= PTB 分配计数 =========================
    std::atomic<size_t> reserved_count_;  // 已保留的 PTB 槽位数（分配阶段）
    std::atomic<size_t> committed_count_; // 已提交的 PTB 槽位数（确认可用）

    // ========================= PTB 指针表 =========================
    static constexpr size_t kMaxPTBs = 128;      // 最多支持的线程/槽位数
    PerThreadDataBlock *ptb_pointers_[kMaxPTBs]; // 每线程数据块指针表（按槽位索引）

    // ========================= 封印触发标志 =========================
    // 由“写满”的那次 append_ordered 置位；上层可据此触发切段。
    std::atomic<bool> should_seal_{false};
};