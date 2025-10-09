#pragma once
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <thread>
#include <deque>
#include "KVPair.h"
#include "SegmentedBlock.h"
#include "DataBlock.h"
#include "PerThreadDataBlock.h"
#include "SearchLayer.h"

// -----------------------------------------------------------------------------
// SBTree
// -----------------------------------------------------------------------------
class SBTree
{
public:
    // ========================= 构造/析构 =========================
    SBTree();
    ~SBTree(); // 负责释放 DataBlock 链表

    // ========================= 基本操作接口 =========================
    void insert(Key key, Value value);                        // 顺序插入（假设 key 单调递增）
    bool lookup(Key k, Value *out) const;                     // 查找
    size_t scan(Key l, Key r, std::vector<Value> &out) const; // 范围扫描

    void insert_delayed(Key k, Value v); // 延迟插入

    // ========================= 测试/诊断接口 =========================
    bool verify_data_layer(size_t expected_total_keys) const; // 遍历数据层验证正确性
    void flush();                                             // 刷新段 → 数据块（立即转换）

    // ========================= 区间游标 =========================
    class RangeCursor
    {
    public:
        bool next(KVPair *out);                                    // 取下一个元素
        size_t next_batch(std::vector<KVPair> &out, size_t limit); // 批量取元素
        inline bool valid() const noexcept { return blk_ != nullptr; }

    private:
        friend class SBTree;
        RangeCursor(const SBTree *owner, Key l, Key r, DataBlock *start);
        void seek_first_pos_(); // 在当前块内定位到第一个 >= l 的元素

        const SBTree *owner_; // 指向宿主树
        Key l_, r_;
        DataBlock *blk_;  // 当前数据块
        std::size_t idx_; // 当前块内索引
    };
    RangeCursor open_range_cursor(Key l, Key r) const; // 打开区间游标

    // ========================= 索引控制接口 =========================
    uint64_t index_batches_enqueued() const noexcept; // 诊断统计：入队批次数
    uint64_t index_batches_applied() const noexcept;  // 诊断统计：应用批次数
    uint64_t index_items_enqueued() const noexcept;   // 诊断统计：入队数据块数
    uint64_t index_items_applied() const noexcept;    // 诊断统计：已应用数据块数
    std::size_t index_levels() const;                 // 搜索层层数（加锁读取）

private:
    // ========================= 内部辅助 =========================
    void convert_and_append(SegmentedBlock *seg_to_convert);     // 段转换 + 追加数据块
    void index_worker_();                                        // 后台索引线程主循环
    void enqueue_index_task_(std::vector<DataBlock *> &&blocks); // 入队索引任务
    DataBlock *find_candidate_(Key k) const;                     // 在搜索层中查找候选块

    // ========================= 并发控制 =========================
    mutable std::shared_mutex search_mu_;          // 搜索层读写锁
    std::thread index_thread_;                     // 专用索引维护线程
    std::deque<std::vector<DataBlock *>> index_q_; // 索引任务队列
    std::mutex q_mu_;                              // 队列锁
    std::condition_variable q_cv_;                 // 队列条件变量
    std::atomic<bool> index_stop_{false};          // 线程停止标志
    std::atomic<size_t> index_in_flight_{0};       // 正在处理中的批次数

    // ========================= 统计指标 =========================
    std::atomic<uint64_t> idx_batches_enqueued_{0};
    std::atomic<uint64_t> idx_batches_applied_{0};
    std::atomic<uint64_t> idx_items_enqueued_{0};
    std::atomic<uint64_t> idx_items_applied_{0};

    // ========================= 数据层 =========================
    Key max_key_{0};
    std::atomic<SegmentedBlock *> shortcut_; // 当前活跃分段块
    mutable std::mutex data_layer_lock_;     // 数据层链表锁
    DataBlock *data_head_;                   // 数据链表头
    DataBlock *data_tail_;                   // 数据链表尾

    // ========================= 搜索层 =========================
    SearchLayer search_; // 搜索层实例
};
