#pragma once
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <deque>
#include "KVPair.h"
#include "SegmentedBlock.h"
#include "DataBlock.h"
#include "PerThreadDataBlock.h"
#include "SearchLayer.h"

// SBTree 主类
class SBTree
{
public:
    SBTree();
    ~SBTree(); // 新增析构函数，用于释放 DataBlock 链表

    // 顺序插入（key 单增假设）
    void insert(Key key, Value value);

    // 查找
    bool lookup(Key k, Value *out) const;

    // 扫描
    size_t scan(Key l, Key r, std::vector<Value> &out) const;

    // ++ 新增：用于测试的验证方法 ++
    // 遍历整个数据层，验证总数、顺序和数据正确性
    bool verify_data_layer(size_t expected_total_keys) const;

    void flush(); // ++ 新增 flush 方法 ++

    // === 区间游标：遍历 [l, r]，每次产出一个 KVPair ===
    class RangeCursor
    {
    public:
        // 取下一个元素；有则写入 out 并返回 true；否则返回 false
        bool next(KVPair *out);

        // 批量拉取：最多取 limit 条，追加到 out，返回本次追加条数
        size_t next_batch(std::vector<KVPair> &out, size_t limit);

        // 是否还有元素可读
        inline bool valid() const noexcept { return blk_ != nullptr; }

    private:
        friend class SBTree;
        RangeCursor(const SBTree *owner, Key l, Key r, DataBlock *start)
            : owner_(owner), l_(l), r_(r), blk_(start), idx_(0)
        {
            if (!blk_ || blk_->min_key() > r_)
            {
                blk_ = nullptr;
                return;
            }
            seek_first_pos_();
        }

        void seek_first_pos_(); // 在当前块内把 idx_ 定位到 >= l_ 的第一条

    private:
        const SBTree *owner_; // 单线程阶段仅作保留
        Key l_, r_;
        DataBlock *blk_;  // 当前块（只借用，不拥有）
        std::size_t idx_; // 当前块内位置
    };

    // 打开 [l, r] 的区间游标
    RangeCursor open_range_cursor(Key l, Key r) const;

    // 索引刷到位（barrier）
    void flush_index();

    // 仅测试/诊断：读取统计值与当前索引层数
    uint64_t index_batches_enqueued() const noexcept;
    uint64_t index_batches_applied() const noexcept;
    uint64_t index_items_enqueued() const noexcept;
    uint64_t index_items_applied() const noexcept;
    std::size_t index_levels() const; // 读锁保护后转发 search_.levels()

private:
    // ++ 新增私有辅助函数 ++
    void convert_and_append(SegmentedBlock *seg_to_convert);

    // --- 读侧保护搜索层（先求稳，后续可优化到无锁/RCU） ---
    mutable std::shared_mutex search_mu_;

    // --- 索引专线线程 ---
    std::thread index_thread_;
    std::deque<std::vector<DataBlock *>> index_q_;
    std::mutex q_mu_;
    std::condition_variable q_cv_;
    std::atomic<bool> index_stop_{false};

    // 可选：用于 flush barrier 的飞行计数
    std::atomic<size_t> index_in_flight_{0};

    // 后台线程与入队
    void index_worker_();
    void enqueue_index_task_(std::vector<DataBlock *> &&blocks);

    // 读侧包装（避免到处手动加锁）
    DataBlock *find_candidate_locked_(Key k) const;

    // --- 统计指标（仅测试/诊断用） ---
    std::atomic<uint64_t> idx_batches_enqueued_{0};
    std::atomic<uint64_t> idx_batches_applied_{0};
    std::atomic<uint64_t> idx_items_enqueued_{0};
    std::atomic<uint64_t> idx_items_applied_{0};

    Key max_key_{0};
    std::atomic<SegmentedBlock *> shortcut_; // 指向当前活跃分段块

    // ++ 新增成员，用于管理 DataBlock 链表 ++
    mutable std::mutex data_layer_lock_; // ++ 改为 mutable ++
    DataBlock *data_head_;               // 数据块链表的头指针
    DataBlock *data_tail_;               // 数据块链表的尾指针

    // 搜索层
    SearchLayer search_;
};