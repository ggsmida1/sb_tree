#include "SBTree.h"

SBTree::SBTree()
    : shortcut_(new SegmentedBlock()) {}

void SBTree::insert(Key key, Value value)
{
    while (true)
    {
        SegmentedBlock *seg = shortcut_.load(std::memory_order_acquire);
        if (seg && seg->append_ordered(key, value))
        {
            // 成功写入
            return;
        }

        // 失败：该线程的 PTB 满或本段无空槽 ——> 试图切到新分段块
        SegmentedBlock *new_seg = new SegmentedBlock();

        SegmentedBlock *expected = seg;
        if (shortcut_.compare_exchange_weak(
                expected, new_seg,
                std::memory_order_acq_rel, std::memory_order_acquire))
        {
            // 切换成功后再试一次写入（应该会成功）
            (void)new_seg->append_ordered(key, value);
            return;
        }
        else
        {
            // 已有其它线程完成切换，丢弃自己建的新段并重试
            delete new_seg;
        }
    }
}
