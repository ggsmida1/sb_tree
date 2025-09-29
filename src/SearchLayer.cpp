#include "SearchLayer.h"
#include "DataBlock.h"
#include <atomic>
#include <algorithm>

// ========================= 内部断言 =========================
// 确认传入的一批 DataBlock 按 min_key 非降
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

// ========================= 构造 =========================
SearchLayer::SearchLayer(std::size_t fanout)
    : fanout_(fanout)
{
    assert(fanout_ >= 2 && "fanout must be >= 2");
    auto init = std::make_shared<SearchSnapshot>();
    std::atomic_store(&snapshot_, std::static_pointer_cast<const SearchSnapshot>(init));
}

// ========================= 快照维护 =========================
void SearchLayer::rebuild_snapshot_()
{
    auto snap = std::make_shared<SearchSnapshot>();
    snap->L0 = L0_;
    snap->L = L_;
    std::atomic_store(&snapshot_, std::static_pointer_cast<const SearchSnapshot>(snap));
}

std::size_t SearchLayer::levels_snapshot() const noexcept
{
    auto snap = std::atomic_load(&snapshot_);
    if (!snap)
        return 0;
    return snap->L.empty() ? (snap->L0.empty() ? 0u : 1u) : snap->L.size() + 1;
}

// ========================= 清空 =========================
void SearchLayer::clear()
{
    L0_.clear();
    L_.clear();
    promoted_.clear();
}

// ========================= 内部二分 =========================
std::size_t SearchLayer::upper_floor_index_(const std::vector<NodeEnt> &arr, Key k) noexcept
{
    std::size_t lo = 0, hi = arr.size(), pos = static_cast<std::size_t>(-1);
    while (lo < hi)
    {
        std::size_t mid = lo + ((hi - lo) >> 1);
        if (arr[mid].min_key <= k)
        {
            pos = mid;
            lo = mid + 1;
        }
        else
            hi = mid;
    }
    return pos;
}

std::size_t SearchLayer::leaf_floor_index_(const std::vector<LeafEnt> &arr,
                                           std::size_t lo, std::size_t hi,
                                           Key k) noexcept
{
    if (lo >= hi)
        return static_cast<std::size_t>(-1);
    std::size_t L = lo, R = hi, pos = static_cast<std::size_t>(-1);
    while (L < R)
    {
        std::size_t mid = L + ((R - L) >> 1);
        if (arr[mid].min_key <= k)
        {
            pos = mid;
            L = mid + 1;
        }
        else
            R = mid;
    }
    return pos;
}

// ========================= 晋升 =========================
void SearchLayer::promote_from_level_(std::size_t level)
{
    const std::size_t F = fanout_;
    auto level_size = [&](std::size_t lv)
    { return (lv == 0) ? L0_.size() : L_[lv - 1].size(); };
    auto level_min_key_at = [&](std::size_t lv, std::size_t idx)
    { return (lv == 0) ? L0_[idx].min_key : L_[lv - 1][idx].min_key; };

    std::size_t lv = level;
    for (;;)
    {
        if (promoted_.size() <= lv)
            promoted_.resize(lv + 1, 0);
        std::size_t p = promoted_[lv];
        std::size_t sz = level_size(lv);

        if (sz >= p + F)
        {
            if (L_.size() <= lv)
                L_.resize(lv + 1);
            if (promoted_.size() <= lv + 1)
                promoted_.resize(lv + 2, 0);
            const std::size_t child_begin = p;
            L_[lv].push_back(NodeEnt{level_min_key_at(lv, child_begin), child_begin, F});
            p += F;
            promoted_[lv] = p;
            continue;
        }
        if (L_.size() <= lv)
            break;
        if (promoted_.size() <= lv + 1)
            break;
        std::size_t parent_sz = L_[lv].size();
        std::size_t parent_p = promoted_[lv + 1];
        if (parent_sz >= parent_p + F)
        {
            ++lv;
            continue;
        }
        break;
    }
}

// ========================= 追加 =========================
void SearchLayer::append_run(const std::vector<DataBlock *> &blocks)
{
    if (blocks.empty())
        return;
    debug_verify_sorted_leaf_run_(blocks);
#ifndef NDEBUG
    if (!L0_.empty())
        assert(L0_.back().min_key <= blocks.front()->min_key());
#endif

    for (auto *b : blocks)
        L0_.push_back(LeafEnt{b->min_key(), b});

    if (promoted_.size() < 1)
        promoted_.resize(1, 0);
    promote_from_level_(0);

#ifndef NDEBUG
    for (std::size_t i = 1; i < L0_.size(); ++i)
        assert(L0_[i - 1].min_key <= L0_[i].min_key);
    for (std::size_t lv = 0; lv < promoted_.size(); ++lv)
        assert(promoted_[lv] <= ((lv == 0) ? L0_.size() : L_[lv - 1].size()));
#endif

    rebuild_snapshot_();
}

// ========================= 查找 =========================
DataBlock *SearchLayer::find_candidate(Key k) const noexcept
{
    auto snap = std::atomic_load(&snapshot_);
    if (!snap || snap->L0.empty())
        return nullptr;
    const auto &L0 = snap->L0;
    const auto &L = snap->L;
    const auto npos = static_cast<std::size_t>(-1);

    if (L.empty())
    {
        std::size_t pos = leaf_floor_index_(L0, 0, L0.size(), k);
        return (pos == npos) ? nullptr : L0[pos].ptr;
    }

    std::size_t top = L.size() - 1;
    const auto &topv = L[top];
    std::size_t idx = upper_floor_index_(topv, k);
    if (idx == npos)
        return nullptr;

    std::size_t lo = topv[idx].child_begin;
    std::size_t hi = lo + topv[idx].child_count;

    for (std::size_t lv = top; lv > 0; --lv)
    {
        const auto &nodes = L[lv - 1];
        std::size_t Lb = lo, Rb = hi, pos = npos;
        while (Lb < Rb)
        {
            std::size_t mid = Lb + ((Rb - Lb) >> 1);
            if (nodes[mid].min_key <= k)
            {
                pos = mid;
                Lb = mid + 1;
            }
            else
                Rb = mid;
        }
        if (pos == npos)
            return nullptr;
        lo = nodes[pos].child_begin;
        hi = lo + nodes[pos].child_count;
    }

    std::size_t leaf_pos = leaf_floor_index_(L0, lo, hi, k);
    return (leaf_pos == npos) ? nullptr : L0[leaf_pos].ptr;
}