#include "SegmentedBlock.h"

SegmentedBlock::SegmentedBlock()
    : status_(BlockStatus::ACTIVE),
      min_key_(UINT64_MAX),
      next_block_(nullptr),
      reserved_count_(0),
      committed_count_(0)
{
    std::fill(std::begin(ptb_pointers_), std::end(ptb_pointers_), nullptr);
}

bool SegmentedBlock::append_ordered(Key k, Value v)
{
    int slot = get_or_create_slot_for_this_thread_();
    if (slot < 0)
        return false; // 没有可用 PTB 槽位，需要切换到新分段块

    PerThreadDataBlock *ptb = ptb_pointers_[static_cast<size_t>(slot)];
    if (!ptb)
        return false; // 理论不该发生

    if (!ptb->Insert(k, v))
    {
        // 该线程的 PTB 已满，本分段块对该线程来说写不动了
        return false;
    }
    // 第一次写入或更小的 key 时更新 min_key_
    if (k < min_key_)
        min_key_ = k;
    return true;
}

int SegmentedBlock::get_or_create_slot_for_this_thread_()
{
    // 线程本地缓存槽位，下次直接复用
    static thread_local int tls_slot = -1;
    if (tls_slot >= 0)
        return tls_slot;

    // 线性找空槽，简单起见用互斥保护（后续需要可再做无锁/原子优化）
    std::lock_guard<std::mutex> g(lock_);
    for (size_t i = 0; i < kMaxPTBs; ++i)
    {
        if (ptb_pointers_[i] == nullptr)
        {
            ptb_pointers_[i] = new PerThreadDataBlock();
            ++reserved_count_;
            ++committed_count_; // 这里把“占用即提交”合并处理（只做顺序插入足够）
            tls_slot = static_cast<int>(i);
            return tls_slot;
        }
    }
    // 没有空位了，本分段块需要外层切换到新分段块
    return -1;
}
