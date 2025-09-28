#include "SegmentedBlock.h"
#include <algorithm>

SegmentedBlock::SegmentedBlock()
    : status_(BlockStatus::ACTIVE),
      min_key_(UINT64_MAX),
      next_block_(nullptr),
      reserved_count_(0),
      committed_count_(0)
{
    std::fill(std::begin(ptb_pointers_), std::end(ptb_pointers_), nullptr);
}

// 注意：析构函数很重要，需要释放所有 PTB
SegmentedBlock::~SegmentedBlock()
{
    for (size_t i = 0; i < kMaxPTBs; ++i)
    {
        delete ptb_pointers_[i];
    }
}

bool SegmentedBlock::append_ordered(Key k, Value v)
{
    // 仅 ACTIVE 时允许写入
    if (status_.load(std::memory_order_acquire) != BlockStatus::ACTIVE)
    {
        return false;
    }
    // （可选）PTB 槽位阈值
    if (reserved_count_.load(std::memory_order_relaxed) >= kMaxPTBs)
    {
        return false;
    }

    int slot = get_or_create_slot_for_this_thread_();
    if (slot < 0)
        return false;

    PerThreadDataBlock *ptb = ptb_pointers_[slot];
    if (ptb == nullptr)
    {
        ptb = new PerThreadDataBlock();
        ptb_pointers_[slot] = ptb;
        reserved_count_.fetch_add(1, std::memory_order_relaxed);
    }

    if (ptb->IsFull())
    {
        return false; // 已满则交给上层切段
    }

    // —— 真正写入 ——（你原有的 Insert）
    if (!ptb->Insert(k, v))
    {
        return false;
    }

    // 更新 min_key_（你原有的逻辑）
    Key old_min = min_key_.load(std::memory_order_relaxed);
    while (k < old_min &&
           !min_key_.compare_exchange_weak(old_min, k,
                                           std::memory_order_release,
                                           std::memory_order_relaxed))
    {
    }

    committed_count_.fetch_add(1, std::memory_order_relaxed);

    // === 新增：如果“刚好被这次写入填满”，置 should_seal_ = true ===
    if (ptb->IsFull())
    {
        should_seal_.store(true, std::memory_order_release);
    }

    return true;
}

void SegmentedBlock::seal()
{
    // --- 新增：把段置为 CONVERT（幂等），用于封印 ---
    BlockStatus expected = BlockStatus::ACTIVE;
    status_.compare_exchange_strong(
        expected, BlockStatus::CONVERT,
        std::memory_order_acq_rel, std::memory_order_acquire);
    // 如果原本就不是 ACTIVE（比如已是 CONVERT/CONVERTED），保持幂等，无需报错
}

// ++ 新增方法的实现 ++
std::vector<KVPair> SegmentedBlock::collect_and_sort_data()
{
    // --- 新增：转换阶段的互斥保护 ---
    std::lock_guard<std::mutex> g(lock_);

    // 最保险：确保处于 CONVERT（非必须，但利于诊断）
    if (status_.load(std::memory_order_acquire) == BlockStatus::ACTIVE)
    {
        // 若还没封印，先封印（幂等）
        seal();
    }

    std::vector<KVPair> all_data;

    // 1. 预计算总大小并 reserve，提升性能
    size_t total_entries = 0;
    for (size_t i = 0; i < kMaxPTBs; ++i)
    {
        if (ptb_pointers_[i])
        {
            total_entries += ptb_pointers_[i]->GetNumEntries();
        }
    }
    all_data.reserve(total_entries);

    // 2. 收集所有 PTB 的数据
    for (size_t i = 0; i < kMaxPTBs; ++i)
    {
        if (ptb_pointers_[i])
        {
            const KVPair *ptb_data = ptb_pointers_[i]->GetData();
            const size_t num_entries = ptb_pointers_[i]->GetNumEntries();
            all_data.insert(all_data.end(), ptb_data, ptb_data + num_entries);
        }
    }

    // 3. 对收集到的所有数据按 key 进行排序
    std::sort(all_data.begin(), all_data.end(),
              [](const KVPair &a, const KVPair &b)
              {
                  return a.key < b.key;
              });

    return all_data;
}

int SegmentedBlock::get_or_create_slot_for_this_thread_()
{
    // 线程本地缓存槽位，下次直接复用
    static thread_local int tls_slot = -1;

    // 关键修复：在使用缓存的 tls_slot 之前，必须检查它对于“当前这个”
    // SegmentedBlock 实例是否有效（即指针是否已分配）。
    if (tls_slot >= 0 && static_cast<size_t>(tls_slot) < kMaxPTBs && ptb_pointers_[tls_slot] != nullptr)
    {
        // 缓存有效，直接返回
        return tls_slot;
    }

    // 缓存无效（可能属于上一个 SegmentedBlock 实例），需要重新分配
    std::lock_guard<std::mutex> g(lock_);
    for (size_t i = 0; i < kMaxPTBs; ++i)
    {
        if (ptb_pointers_[i] == nullptr)
        {
            ptb_pointers_[i] = new PerThreadDataBlock();
            ++reserved_count_;
            ++committed_count_;
            tls_slot = static_cast<int>(i); // 更新缓存
            return tls_slot;
        }
    }

    // 没有空位了，本分段块需要外层切换到新分段块
    return -1;
}