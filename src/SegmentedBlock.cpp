#include "SegmentedBlock.h"
#include <algorithm>

// ========================= 构造/析构 =========================
SegmentedBlock::SegmentedBlock()
    : status_(BlockStatus::ACTIVE),
      min_key_(UINT64_MAX),
      next_block_(nullptr),
      reserved_count_(0),
      committed_count_(0)
{
    std::fill(std::begin(ptb_pointers_), std::end(ptb_pointers_), nullptr);
}

// 析构时释放所有 PTB
SegmentedBlock::~SegmentedBlock()
{
    for (size_t i = 0; i < kMaxPTBs; ++i)
    {
        delete ptb_pointers_[i];
    }
}

// ========================= 写入接口 =========================
// 在当前分段块中顺序追加一条 KV
bool SegmentedBlock::append_ordered(Key k, Value v)
{
    if (status_.load(std::memory_order_acquire) != BlockStatus::ACTIVE)
        return false; // 仅 ACTIVE 状态允许写入

    if (reserved_count_.load(std::memory_order_relaxed) >= kMaxPTBs)
        return false; // PTB 槽位已达上限

    int slot = get_or_create_slot_for_this_thread_();
    if (slot < 0)
        return false;

    PerThreadDataBlock *ptb = ptb_pointers_[slot];
    if (!ptb)
    {
        ptb = new PerThreadDataBlock();
        ptb_pointers_[slot] = ptb;
        reserved_count_.fetch_add(1);
    }

    if (ptb->IsFull())
        return false; // 当前 PTB 已满

    if (!ptb->Insert(k, v))
        return false; // 插入失败

    // 更新 min_key_
    Key old_min = min_key_.load(std::memory_order_relaxed);
    while (k < old_min &&
           !min_key_.compare_exchange_weak(old_min, k,
                                           std::memory_order_release,
                                           std::memory_order_relaxed))
    {
    }

    committed_count_.fetch_add(1);

    // 如果刚好写满，触发 should_seal_
    if (ptb->IsFull())
        should_seal_.store(true, std::memory_order_release);

    return true;
}

// ========================= 状态管理 =========================
// 将段状态从 ACTIVE → CONVERT，用于封印
void SegmentedBlock::seal()
{
    BlockStatus expected = BlockStatus::ACTIVE;
    status_.compare_exchange_strong(expected, BlockStatus::CONVERT,
                                    std::memory_order_acq_rel,
                                    std::memory_order_acquire);
}

// ========================= 数据收集 =========================
// 收集并排序本段所有 PTB 的数据
std::vector<KVPair> SegmentedBlock::collect_and_sort_data()
{
    std::lock_guard<std::mutex> g(lock_);

    if (status_.load(std::memory_order_acquire) == BlockStatus::ACTIVE)
        seal(); // 若还未封印，先封印

    std::vector<KVPair> all_data;
    size_t total_entries = 0;
    for (size_t i = 0; i < kMaxPTBs; ++i)
        if (ptb_pointers_[i])
            total_entries += ptb_pointers_[i]->GetNumEntries();
    all_data.reserve(total_entries);

    for (size_t i = 0; i < kMaxPTBs; ++i)
        if (ptb_pointers_[i])
        {
            const KVPair *ptb_data = ptb_pointers_[i]->GetData();
            size_t n = ptb_pointers_[i]->GetNumEntries();
            all_data.insert(all_data.end(), ptb_data, ptb_data + n);
        }

    std::sort(all_data.begin(), all_data.end(),
              [](const KVPair &a, const KVPair &b)
              { return a.key < b.key; });
    return all_data;
}

// ========================= 内部辅助 =========================
// 获取或为当前线程分配 PTB 槽位
int SegmentedBlock::get_or_create_slot_for_this_thread_()
{
    static thread_local int tls_slot = -1;
    if (tls_slot >= 0 && (size_t)tls_slot < kMaxPTBs && ptb_pointers_[tls_slot])
        return tls_slot; // 缓存有效

    std::lock_guard<std::mutex> g(lock_);
    for (size_t i = 0; i < kMaxPTBs; ++i)
    {
        if (!ptb_pointers_[i])
        {
            ptb_pointers_[i] = new PerThreadDataBlock();
            ++reserved_count_;
            ++committed_count_;
            tls_slot = static_cast<int>(i);
            return tls_slot;
        }
    }
    return -1; // 无可用槽位
}