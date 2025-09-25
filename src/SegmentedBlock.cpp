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
    int slot = get_or_create_slot_for_this_thread_();
    if (slot < 0)
    {
        return false; // 本分段块已无空槽位
    }

    PerThreadDataBlock *ptb = ptb_pointers_[static_cast<size_t>(slot)];

    // 之前的实现中，get_or_create...保证了ptb非空，但现在由于缓存策略，
    // 我们在此处再次检查ptb->Insert的返回值就足够了。
    if (!ptb->Insert(k, v))
    {
        // 该线程的 PTB 已满
        return false;
    }

    // 第一次写入或更小的 key 时更新 min_key_
    // 使用 relaxed 内存序即可，因为它不用于同步
    if (k < min_key_.load(std::memory_order_relaxed))
    {
        min_key_.store(k, std::memory_order_relaxed);
    }

    return true;
}

// ++ 新增方法的实现 ++
std::vector<KVPair> SegmentedBlock::collect_and_sort_data()
{
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