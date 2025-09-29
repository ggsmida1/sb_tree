#pragma once
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cassert>
#include <memory>
#include "KVPair.h" // 定义 Key 类型

class DataBlock; // 前向声明

// -----------------------------------------------------------------------------
// SearchLayer
// -----------------------------------------------------------------------------
// 作用：
// - 数据层上方的搜索层，类似 B+ 树的索引部分。
// - L0 层（叶层）保存 DataBlock 的摘要 {min_key, 指针}。
// - L1/L2/... 层是内层节点，按固定扇出 fanout 聚合子节点。
// - 提供批量追加（append_run）和候选定位（find_candidate）。
// 并发语义：
// - 搜索层的写入由后台专用线程维护（append_run 时批量晋升）；
// - 其他工作线程只读搜索层，可并发安全访问；
// - 这样避免了搜索层上的写入冲突，整体并发由数据层承担。
// 不变式：
// - L0 中条目按 min_key 非降；
// - 内层节点的 min_key 等于其覆盖的第一个子节点的 min_key；
// - promoted_ 记录各层已经完成晋升的位置。
// -----------------------------------------------------------------------------
class SearchLayer
{
public:
    // ----------------------------- 公共结构体 --------------------------------
    struct LeafEnt
    {
        Key min_key;    // 对应 DataBlock 的最小 key
        DataBlock *ptr; // 指向数据块
    };

    struct NodeEnt
    {
        Key min_key;             // 覆盖区间的最小 key（取首子节点）
        std::size_t child_begin; // 在下层数组中的起始下标
        std::size_t child_count; // 覆盖的子节点数量（= fanout_）
    };

    struct SearchSnapshot
    {
        std::vector<LeafEnt> L0;             // 叶层快照
        std::vector<std::vector<NodeEnt>> L; // 内层快照
    };

    // ----------------------------- 构造/析构 --------------------------------
    explicit SearchLayer(std::size_t fanout = 64);
    ~SearchLayer() = default;

    SearchLayer(const SearchLayer &) = delete;
    SearchLayer &operator=(const SearchLayer &) = delete;
    SearchLayer(SearchLayer &&) = default;
    SearchLayer &operator=(SearchLayer &&) = default;

    // ----------------------------- 追加接口 ---------------------------------
    // 批量追加：一次段转换产出的 DataBlock* run（已按 min_key 有序）
    void append_run(const std::vector<DataBlock *> &blocks);

    // ----------------------------- 查询接口 ---------------------------------
    // 查找候选：返回“最后一个 min_key <= k”的 DataBlock*，否则 nullptr
    DataBlock *find_candidate(Key k) const noexcept;

    // ----------------------------- 工具/状态 --------------------------------
    bool empty() const noexcept { return L0_.empty(); }           // 是否为空
    std::size_t leaf_size() const noexcept { return L0_.size(); } // 叶层条目数
    std::size_t levels() const noexcept { return L_.size() + 1; } // 总层数（含叶层）
    std::size_t fanout() const noexcept { return fanout_; }       // 返回扇出因子
    void clear();                                                 // 清空全部内容

    // 返回当前快照的层级数（测试/调试用）
    std::size_t levels_snapshot() const noexcept;

private:
    // ----------------------------- 内部帮助 ---------------------------------
    static void debug_verify_sorted_leaf_run_(const std::vector<DataBlock *> &blocks);
    void promote_from_level_(std::size_t level); // 从某层开始尝试晋升

    // 二分查找：父层节点数组，返回“最后一个 min_key <= k”的下标
    static std::size_t upper_floor_index_(const std::vector<NodeEnt> &arr, Key k) noexcept;

    // 二分查找：叶层区间 [lo,hi)，返回“最后一个 min_key <= k”的下标
    static std::size_t leaf_floor_index_(const std::vector<LeafEnt> &arr,
                                         std::size_t lo, std::size_t hi,
                                         Key k) noexcept;

    // 重新构建快照（仅写线程调用）
    void rebuild_snapshot_();

private:
    // ----------------------------- 成员变量 ---------------------------------
    std::vector<LeafEnt> L0_;             // 叶层
    std::vector<std::vector<NodeEnt>> L_; // 内层（L1,L2,...）
    std::vector<std::size_t> promoted_;   // 各层的晋升进度指针

    std::size_t fanout_ = 64; // 固定扇出

    std::shared_ptr<const SearchSnapshot> snapshot_; // 快照指针
};