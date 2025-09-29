#pragma once
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cassert>
#include <memory>
#include "KVPair.h" // for Key

class DataBlock; // fwd decl

// ============================================================================
// SearchLayer — 批量晋升 / 只追加 的 B+ 风格搜索层（单线程阶段）
// ----------------------------------------------------------------------------
// 设计要点：
//  - L0（叶层）：每个条目对应一个 DataBlock 的摘要 {min_key, ptr}
//  - L1/L2/…（内层）：父条目汇总下层“连续 F 个”孩子，记录
//        {min_key_of_group, child_begin, child_count=F}
//  - append_run(blocks)：先把本次 run 的块追加到 L0，然后自下而上按扇出 F
//    对“新增的条目”做批量晋升；不足 F 的“尾巴”保留到下次。
//  - find_candidate(k)：若有内层，从最高层开始二分下钻到 L0；否则直接在 L0
//    二分得到候选块指针；块内未命中由上层沿数据层 next 兜底。
//  - 单线程阶段：本类不做并发同步；后续可在 append_run 外层做队列＋专线线程。
// ============================================================================
class SearchLayer
{
public:
    // ----------------------------- 公共类型 ---------------------------------
    struct LeafEnt
    {
        Key min_key;    // 叶条目的下界（对应 DataBlock::min_key）
        DataBlock *ptr; // 指向数据块
    };
    struct NodeEnt
    {
        Key min_key;             // 该父条目覆盖的子区间的最小 key（取首子条目）
        std::size_t child_begin; // 在下层数组中的起始下标（连续）
        std::size_t child_count; // = fanout_（固定扇出）
    };

    // SearchLayer.h
    struct SearchSnapshot
    {
        std::vector<LeafEnt> L0;             // 叶层快照
        std::vector<std::vector<NodeEnt>> L; // 内层快照
    };

    // index_levels() 读快照
    std::size_t levels_snapshot() const noexcept;

    // ----------------------------- 构造/基本 ---------------------------------
    explicit SearchLayer(std::size_t fanout = 64);
    ~SearchLayer() = default;

    SearchLayer(const SearchLayer &) = delete;
    SearchLayer &operator=(const SearchLayer &) = delete;
    SearchLayer(SearchLayer &&) = default;
    SearchLayer &operator=(SearchLayer &&) = default;

    // ----------------------------- 追加/查询 ---------------------------------
    // 批量追加：一次段转换产出的 run（blocks 已按 min_key 非降，且已整批尾插到数据层）
    void append_run(const std::vector<DataBlock *> &blocks);

    // 候选定位：返回“最后一个 min_key <= k”的 DataBlock*；若没有，返回 nullptr。
    DataBlock *find_candidate(Key k) const noexcept;

    // ----------------------------- 工具/状态 ---------------------------------
    inline bool empty() const noexcept { return L0_.empty(); }
    inline std::size_t leaf_size() const noexcept { return L0_.size(); }
    inline std::size_t levels() const noexcept { return L_.size() + 1; } // 含叶层
    inline std::size_t fanout() const noexcept { return fanout_; }
    void clear();

private:
    // ----------------------------- 内部帮助 ---------------------------------
    // 断言：本次 run 的块按 min_key 非降
    static void debug_verify_sorted_leaf_run_(const std::vector<DataBlock *> &blocks);

    // 从给定层（level=0 表示叶层 L0_）开始，尽可能把“新增的条目”按扇出 F 晋升到上一层
    void promote_from_level_(std::size_t level);

    // 在父层 vector 上做“最后一个 min_key <= k”的二分；返回下标或 (size_t)-1
    static std::size_t upper_floor_index_(const std::vector<NodeEnt> &arr, Key k) noexcept;

    // 在叶层指定区间 [lo, hi) 做“最后一个 min_key <= k”的二分；返回下标或 (size_t)-1
    static std::size_t leaf_floor_index_(const std::vector<LeafEnt> &arr,
                                         std::size_t lo, std::size_t hi, Key k) noexcept;

private:
    // 层级存储
    std::vector<LeafEnt> L0_;             // 叶层
    std::vector<std::vector<NodeEnt>> L_; // L_[0]=L1, L_[1]=L2, …（可能为空）

    // 每层“已晋升到的位置”（推进指针）：
    // promoted_[0] 对应 L0_，promoted_[i] 对应 L_[i-1]
    std::vector<std::size_t> promoted_;

    std::size_t fanout_ = 64;

    // === 新增：快照指针（C++17：用 atomic_* 操作 shared_ptr）===
    std::shared_ptr<const SearchSnapshot> snapshot_;

    // === 新增：在写入/晋升后重建快照（仅写线程调用）===
    void rebuild_snapshot_();
};
