#include "SBTree.h"
#include <vector>
#include <iostream>
#include <algorithm> // for std::sort if needed, and std::min
#include <cassert>

// ========================= 构造/析构 =========================
SBTree::SBTree()
    : max_key_{0},
      active_buffer_(nullptr), // 初始时没有活跃缓冲区
      data_head_(nullptr),
      data_tail_(nullptr)
{
    // 构造函数变得非常简单，不再需要启动任何后台线程。
}

SBTree::~SBTree()
{
    // 1. 确保所有缓冲的数据都被转换和写入
    flush();

    // 2. 释放数据层链表
    DataBlock *cur = data_head_;
    while (cur)
    {
        DataBlock *nxt = cur->next();
        delete cur;
        cur = nxt;
    }
    data_head_ = data_tail_ = nullptr;

    // 3. 析构函数不再需要处理线程和队列
}

// ========================= 内部核心辅助 =========================

// 这是新的核心方法，负责将活跃缓冲区的数据转换为 DataBlock，并更新索引
void SBTree::convert_active_buffer_()
{
    // 如果没有缓冲区或者缓冲区是空的，则无需转换
    if (!active_buffer_ || active_buffer_->GetNumEntries() == 0)
    {
        if (active_buffer_)
        {
            delete active_buffer_;
            active_buffer_ = nullptr;
        }
        return;
    }

    // 1. 从缓冲区收集数据
    const KVPair *buffer_data = active_buffer_->GetData();
    size_t num_entries = active_buffer_->GetNumEntries();

    // 注意：因为是单线程且我们假设key单调递增写入，
    // 所以 active_buffer_ 中的数据已经是排序好的，无需 std::sort！
    // 如果未来要支持乱序写入，则需要在这里增加排序步骤。
    // std::vector<KVPair> sorted_data(buffer_data, buffer_data + num_entries);
    // std::sort(sorted_data.begin(), sorted_data.end(), ...);

    // 2. 将有序数据切片成多个 DataBlock
    std::vector<DataBlock *> new_blocks;
    const KVPair *current_pos = buffer_data;
    size_t remaining = num_entries;
    while (remaining > 0)
    {
        auto *new_block = new DataBlock();
        size_t consumed = new_block->build_from_sorted(current_pos, remaining);
        assert(consumed > 0); // 必须消耗掉至少一个元素

        new_blocks.push_back(new_block);
        current_pos += consumed;
        remaining -= consumed;
    }

    // 3. 将新的 DataBlock 链入数据层主链表
    if (!data_tail_) // 如果链表为空
    {
        data_head_ = new_blocks.front();
        data_tail_ = new_blocks.back();
    }
    else
    {
        data_tail_->set_next(new_blocks.front());
        data_tail_ = new_blocks.back();
    }

    // 4. 同步更新搜索层
    search_.append_run(new_blocks);

    // 5. 清理旧的缓冲区
    delete active_buffer_;
    active_buffer_ = nullptr;
}

// ========================= 基本操作 =========================

void SBTree::insert(Key key, Value value)
{
    // 1. 确保有一个活跃的写入缓冲区
    if (active_buffer_ == nullptr)
    {
        active_buffer_ = new PerThreadDataBlock();
    }

    // 2. 如果当前缓冲区满了，先进行转换
    if (active_buffer_->IsFull())
    {
        convert_active_buffer_();
        // 转换后，再次创建一个新的空缓冲区
        active_buffer_ = new PerThreadDataBlock();
    }

    // 3. 插入数据到缓冲区
    bool success = active_buffer_->Insert(key, value);
    assert(success); // 此时缓冲区必然有空间，插入必须成功

    // 4. 更新树的最大 key
    if (key > max_key_)
    {
        max_key_ = key;
    }
}

void SBTree::flush()
{
    // 直接调用转换函数即可，它会处理 active_buffer_ 为空或已为空的情况
    convert_active_buffer_();
}

bool SBTree::lookup(Key k, Value *out) const
{
    // 查找逻辑基本不变，但由于是同步更新，数据总是一致的
    DataBlock *blk = find_candidate_(k);

    // 如果搜索层没找到（比如树是空的，或者key很小）
    // 则从数据层头部开始遍历
    if (!blk)
    {
        blk = data_head_;
    }

    while (blk)
    {
        // 检查当前块是否可能包含key
        if (k >= blk->min_key() && k <= blk->max_key())
        {
            Value v{};
            if (blk->find(k, v))
            {
                if (out)
                    *out = v;
                return true;
            }
        }

        DataBlock *nxt = blk->next();
        // 如果下一个块的最小key已经大于目标k，就没有必要继续了
        if (!nxt || nxt->min_key() > k)
            break;

        blk = nxt;
    }
    return false;
}

size_t SBTree::scan(Key l, Key r, std::vector<Value> &out) const
{
    if (l > r)
        return 0;

    auto cur = open_range_cursor(l, r);
    size_t added = 0;
    KVPair kv;
    while (cur.next(&kv))
    {
        out.push_back(kv.value);
        ++added;
    }
    return added;
}

// ========================= RangeCursor (保持不变) =========================
SBTree::RangeCursor::RangeCursor(const SBTree *owner, Key l, Key r, DataBlock *start)
    : owner_(owner), l_(l), r_(r), blk_(start), idx_(0)
{
    if (!blk_ || blk_->min_key() > r_)
    {
        blk_ = nullptr;
        return;
    }
    seek_first_pos_();
}

void SBTree::RangeCursor::seek_first_pos_()
{
    const std::size_t n = blk_->size();
    std::size_t L = 0, R = n, pos = n;
    while (L < R)
    {
        std::size_t mid = L + ((R - L) >> 1);
        if (blk_->get_entry(mid).key >= l_)
        {
            pos = mid;
            R = mid;
        }
        else
            L = mid + 1;
    }
    idx_ = pos;
    while (blk_ && idx_ >= blk_->size())
    {
        blk_ = blk_->next();
        if (!blk_ || blk_->min_key() > r_)
        {
            blk_ = nullptr;
            break;
        }
        idx_ = 0;
    }
}

bool SBTree::RangeCursor::next(KVPair *out)
{
    if (!blk_)
        return false;

    while (true)
    {
        while (idx_ < blk_->size())
        {
            const KVPair &e = blk_->get_entry(idx_++);
            if (e.key > r_)
            {
                blk_ = nullptr;
                return false;
            }
            if (e.key >= l_)
            {
                if (out)
                    *out = e;
                return true;
            }
        }

        blk_ = blk_->next();
        if (!blk_ || blk_->min_key() > r_)
        {
            blk_ = nullptr;
            return false;
        }
        idx_ = 0;
    }
}

size_t SBTree::RangeCursor::next_batch(std::vector<KVPair> &out, size_t limit)
{
    if (!blk_ || limit == 0)
        return 0;
    size_t added = 0;
    KVPair kv;
    while (added < limit && next(&kv))
    {
        out.push_back(kv);
        ++added;
    }
    return added;
}

SBTree::RangeCursor SBTree::open_range_cursor(Key l, Key r) const
{
    if (l > r)
        return RangeCursor(this, 1, 0, nullptr);
    DataBlock *blk = find_candidate_(l);
    if (!blk)
        blk = data_head_; // 如果索引找不到，从头开始
    return RangeCursor(this, l, r, blk);
}

// ========================= 验证/统计 (简化) =========================
bool SBTree::verify_data_layer(size_t expected_total_keys) const
{
    // 移除了锁
    size_t actual = 0;
    Key last_key = 0;
    DataBlock *cur = data_head_;
    while (cur)
    {
        for (size_t i = 0; i < cur->size(); ++i)
        {
            KVPair e = cur->get_entry(i);
            // 这里可以添加您的验证逻辑
            if (actual > 0 && e.key <= last_key)
                return false; // 验证key是否单调递增
            last_key = e.key;
            ++actual;
        }
        cur = cur->next();
    }
    return actual == expected_total_keys;
}

DataBlock *SBTree::find_candidate_(Key k) const
{
    // 移除了锁
    return search_.find_candidate(k);
}

std::size_t SBTree::index_levels() const
{
    // 移除了锁
    return search_.levels();
}