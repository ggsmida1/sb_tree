#pragma once
#include <atomic>
#include <mutex> // 新增头文件
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

private:
    // ++ 新增私有辅助函数 ++
    void convert_and_append(SegmentedBlock *seg_to_convert);

    Key max_key_{0};
    std::atomic<SegmentedBlock *> shortcut_; // 指向当前活跃分段块

    // ++ 新增成员，用于管理 DataBlock 链表 ++
    mutable std::mutex data_layer_lock_; // ++ 改为 mutable ++
    DataBlock *data_head_;               // 数据块链表的头指针
    DataBlock *data_tail_;               // 数据块链表的尾指针

    // 搜索层
    SearchLayer search_;
};