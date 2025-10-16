#pragma once
#include <vector>
#include "KVPair.h"
// SegmentedBlock.h 已被移除
#include "DataBlock.h"
#include "PerThreadDataBlock.h"
#include "SearchLayer.h"

// -----------------------------------------------------------------------------
// SBTree (最终单线程MVP版)
// -----------------------------------------------------------------------------
// 作用：
//   - SB-Tree 主体类，直接管理搜索层、数据层以及当前的活跃写入缓冲区。
//   - 插入操作将同步完成数据转换和索引更新。
// 并发语义：
//   - 此版本为单线程设计，不包含任何并发控制。
// -----------------------------------------------------------------------------
class SBTree
{
public:
    // ========================= 构造/析构 =========================
    SBTree();
    ~SBTree(); // 负责释放 DataBlock 链表和活跃的写入缓冲区

    // ========================= 基本操作接口 =========================
    void insert(Key key, Value value);
    bool lookup(Key k, Value *out) const;
    size_t scan(Key l, Key r, std::vector<Value> &out) const;

    // ========================= 测试/诊断接口 =========================
    bool verify_data_layer(size_t expected_total_keys) const;
    void flush(); // 强制转换当前活跃的、可能未满的缓冲区

    // ========================= 区间游标 =========================
    class RangeCursor
    {
    public:
        bool next(KVPair *out);
        size_t next_batch(std::vector<KVPair> &out, size_t limit);
        inline bool valid() const noexcept { return blk_ != nullptr; }

    private:
        friend class SBTree;
        RangeCursor(const SBTree *owner, Key l, Key r, DataBlock *start);
        void seek_first_pos_();

        const SBTree *owner_;
        Key l_, r_;
        DataBlock *blk_;
        std::size_t idx_;
    };
    RangeCursor open_range_cursor(Key l, Key r) const;

    // ========================= 索引状态接口 =========================
    std::size_t index_levels() const;

private:
    // ========================= 内部辅助 =========================
    DataBlock *find_candidate_(Key k) const;
    // 新增：内部函数，负责将 active_buffer_ 转换为 DataBlock 并更新索引
    void convert_active_buffer_();

    // ========================= 数据层 =========================
    Key max_key_{0};
    // shortcut_ 被替换为直接管理 PerThreadDataBlock
    PerThreadDataBlock *active_buffer_; // 当前活跃的写入缓冲区
    DataBlock *data_head_;
    DataBlock *data_tail_;

    // ========================= 搜索层 =========================
    SearchLayer search_;
};