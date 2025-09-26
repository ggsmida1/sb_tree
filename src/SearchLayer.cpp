#include "SearchLayer.h"
#include "DataBlock.h"
#include <algorithm>

// ===== 内部断言：本次 run 的块按 min_key 非降 =====
void SearchLayer::debug_verify_sorted_leaf_run_(const std::vector<DataBlock *> &blocks)
{
#ifndef NDEBUG
    if (blocks.empty())
        return;
    Key prev = blocks.front()->min_key();
    for (std::size_t i = 1; i < blocks.size(); ++i)
    {
        Key cur = blocks[i]->min_key();
        assert(prev <= cur && "SearchLayer.append_run(): blocks not sorted by min_key");
        prev = cur;
    }
#endif
}

// ===== 清空所有层 =====
void SearchLayer::clear()
{
    L0_.clear();
    L_.clear();
    promoted_.clear();
}

// ===== 父层整段二分（最后一个 <= k） =====
std::size_t SearchLayer::upper_floor_index_(const std::vector<NodeEnt> &arr, Key k) noexcept
{
    std::size_t lo = 0, hi = arr.size();
    std::size_t pos = static_cast<std::size_t>(-1);
    while (lo < hi)
    {
        std::size_t mid = lo + ((hi - lo) >> 1);
        if (arr[mid].min_key <= k)
        {
            pos = mid;
            lo = mid + 1;
        }
        else
        {
            hi = mid;
        }
    }
    return pos; // = npos 表示没有 <= k
}

// ===== 叶层区间二分（最后一个 <= k） =====
std::size_t SearchLayer::leaf_floor_index_(const std::vector<LeafEnt> &arr,
                                           std::size_t lo, std::size_t hi, Key k) noexcept
{
    if (lo >= hi)
        return static_cast<std::size_t>(-1);
    std::size_t L = lo, R = hi;
    std::size_t pos = static_cast<std::size_t>(-1);
    while (L < R)
    {
        std::size_t mid = L + ((R - L) >> 1);
        if (arr[mid].min_key <= k)
        {
            pos = mid;
            L = mid + 1;
        }
        else
        {
            R = mid;
        }
    }
    return pos;
}

// ---- promote_from_level_：按扇出F批量晋升，安全访问promoted_ ----
void SearchLayer::promote_from_level_(std::size_t level)
{
    const std::size_t F = fanout_;

    auto level_size = [&](std::size_t lv) -> std::size_t
    {
        return (lv == 0) ? L0_.size() : L_[lv - 1].size();
    };
    auto level_min_key_at = [&](std::size_t lv, std::size_t idx) -> Key
    {
        return (lv == 0) ? L0_[idx].min_key : L_[lv - 1][idx].min_key;
    };

    // 从给定层开始，尽可能晋升；避免对 promoted_ 的“悬挂引用”
    std::size_t lv = level;
    for (;;)
    {
        // 确保本层推进指针存在
        if (promoted_.size() <= lv)
            promoted_.resize(lv + 1, 0);

        // 用“值”读取，再在需要时写回，避免 resize 失效引用
        std::size_t p = promoted_[lv];
        std::size_t sz = level_size(lv);

        // 先在当前层尽可能晋升
        if (sz >= p + F)
        {
            // 需要父层容器与父推进指针都存在（这一步可能触发 reallocation）
            if (L_.size() <= lv)
                L_.resize(lv + 1);
            if (promoted_.size() <= lv + 1)
                promoted_.resize(lv + 2, 0);

            const std::size_t child_begin = p;
            L_[lv].push_back(NodeEnt{
                /*min_key*/ level_min_key_at(lv, child_begin),
                /*child_begin*/ child_begin,
                /*child_count*/ F});

            // 更新“值”，然后再写回到 promoted_[lv]
            p += F;
            promoted_[lv] = p;
            continue; // 继续尝试在同一层晋升
        }

        // 本层没法晋升了，看看父层是否因“新增的父条目”凑满 F 可以继续晋升
        if (L_.size() <= lv)
            break; // 没有父层，结束
        if (promoted_.size() <= lv + 1)
            break; // 父推进指针都还没有，结束（不应发生，但安全）

        std::size_t parent_sz = L_[lv].size();
        std::size_t parent_p = promoted_[lv + 1]; // 读“值”，不要引用
        if (parent_sz >= parent_p + F)
        {
            ++lv; // 上移到父层继续同样流程
            continue;
        }
        break; // 父层也不能晋升，整体结束
    }
}

// ---- append_run：叶层批量追加 + 自下而上晋升 ----
void SearchLayer::append_run(const std::vector<DataBlock *> &blocks)
{
    if (blocks.empty())
        return;

    debug_verify_sorted_leaf_run_(blocks);
#ifndef NDEBUG
    if (!L0_.empty())
    {
        assert(L0_.back().min_key <= blocks.front()->min_key() &&
               "append_run(): new run must be >= last leaf min_key");
    }
#endif

    // 叶层批量追加
    L0_.reserve(L0_.size() + blocks.size());
    for (auto *b : blocks)
    {
        L0_.push_back(LeafEnt{b->min_key(), b});
    }

    // 关键：确保叶层推进指针存在
    if (promoted_.size() < 1)
        promoted_.resize(1, 0);

    // 自下而上晋升
    promote_from_level_(0);

#ifndef NDEBUG
    auto level_size = [&](std::size_t lv) -> std::size_t
    {
        return (lv == 0) ? L0_.size() : L_[lv - 1].size();
    };

    // 1) 叶层 min_key 非降
    for (std::size_t i = 1; i < L0_.size(); ++i)
    {
        assert(L0_[i - 1].min_key <= L0_[i].min_key && "L0 not non-decreasing");
    }

    // 2) 推进指针范围
    assert(!promoted_.empty());
    for (std::size_t lv = 0; lv < promoted_.size(); ++lv)
    {
        assert(promoted_[lv] <= level_size(lv) && "promoted_[lv] out of range");
    }

    // 3) 每层父条目：子区间边界与 min_key 对齐
    for (std::size_t lv = 0; lv < L_.size(); ++lv)
    {
        const auto &parent = L_[lv]; // L1=L_[0], L2=L_[1], ...
        const bool child_is_leaf = (lv == 0);
        const std::size_t child_sz = level_size(lv);

        // 父层条目本身非降
        for (std::size_t i = 1; i < parent.size(); ++i)
        {
            assert(parent[i - 1].min_key <= parent[i].min_key && "parent min_key not non-decreasing");
        }

        // 一个取首个孩子 min_key 的小工具
        auto child_first_min = [&](std::size_t child_begin) -> Key
        {
            return child_is_leaf ? L0_[child_begin].min_key
                                 : L_[lv - 1][child_begin].min_key;
        };

        for (const auto &ent : parent)
        {
            assert(ent.child_count == fanout_ && "parent child_count must equal fanout");
            assert(ent.child_begin + ent.child_count <= child_sz && "parent child range OOB");
            assert(ent.min_key == child_first_min(ent.child_begin) &&
                   "parent.min_key must equal first child.min_key");
        }
    }
#endif
}

// ===== 自顶向下查找候选块 =====
DataBlock *SearchLayer::find_candidate(Key k) const noexcept
{
    if (L0_.empty())
        return nullptr;

    const auto npos = static_cast<std::size_t>(-1);

    // 无内层：直接叶层 floor
    if (L_.empty())
    {
        std::size_t pos = leaf_floor_index_(L0_, 0, L0_.size(), k);
        return (pos == npos) ? nullptr : L0_[pos].ptr;
    }

    // 从最高层开始
    std::size_t top = L_.size() - 1;
    std::size_t lo = 0, hi = 0;

    // 顶层父项（覆盖的是下一层的连续区间）
    const auto &topv = L_[top];
    std::size_t idx = upper_floor_index_(topv, k);
    if (idx == npos)
    {
        // 比第一个父项的 min_key 还小 → 无候选
        return nullptr;
    }
    lo = topv[idx].child_begin;
    hi = lo + topv[idx].child_count;

    // 逐层向下选择“子父项”
    for (std::size_t lv = top; lv > 0; --lv)
    {
        const auto &nodes = L_[lv - 1];
        std::size_t L = lo, R = hi, pos = npos;
        while (L < R)
        {
            std::size_t mid = L + ((R - L) >> 1);
            if (nodes[mid].min_key <= k)
            {
                pos = mid;
                L = mid + 1;
            }
            else
            {
                R = mid;
            }
        }
        if (pos == npos)
        {
            // k 小于该父项子区间的第一个孩子 → 无候选
            return nullptr;
        }
        lo = nodes[pos].child_begin;
        hi = lo + nodes[pos].child_count;
    }

    // 到达叶层：在 [lo, hi) 上做 floor
    std::size_t leaf_pos = leaf_floor_index_(L0_, lo, hi, k);
    return (leaf_pos == npos) ? nullptr : L0_[leaf_pos].ptr;
}
