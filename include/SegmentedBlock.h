#pragma once
#include <atomic>
#include <cstdint>
#include <mutex>
#include <algorithm>
#include <iterator>
#include <vector>
#include "KVPair.h"
#include "PerThreadDataBlock.h"

// 分段块中,定义块的状态。
enum class BlockStatus : uint8_t
{
    // 正在接受插入
    ACTIVE,
    // 正在被转换,不接受插入
    CONVERT,
    // 转换完成
    CONVERTED,
};

// 分段块
class SegmentedBlock
{
public:
    SegmentedBlock();
    ~SegmentedBlock();

    // 顺序插入：为“当前线程”获取/创建一个 PTB，然后尝试写入。
    // 返回 true 表示写入成功；返回 false 表示本分段块已不再接受写入
    // （例如没有空槽位或该线程的 PTB 已满，外层应切换到新分段块）。
    bool append_ordered(Key k, Value v);

    // 收集所有 PTB 中的数据，合并到一个 vector 并排序后返回。
    std::vector<KVPair> collect_and_sort_data();

private:
    // 为“当前线程”分配一个专属 PTB 槽位（第一次调用时分配）。
    // 返回槽位下标 [0, kMaxPTBs)，失败返回 -1。
    int get_or_create_slot_for_this_thread_();

    // 记录块状态
    std::atomic<BlockStatus> status_;
    // 记录块最小Key;
    std::atomic<Key> min_key_;
    // 指向数据层下一个块的指针
    SegmentedBlock *next_block_;
    // 用于块转换过程中的锁
    std::mutex lock_;

    // 用于PTB分配的原子计数器
    std::atomic<size_t> reserved_count_;
    std::atomic<size_t> committed_count_;

    // 指向每线程数据块的指针
    static constexpr size_t kMaxPTBs = 128;
    PerThreadDataBlock *ptb_pointers_[kMaxPTBs];
};
