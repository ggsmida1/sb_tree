#include "SBTree.h"
#include <vector>
#include <iostream>

// ========================= 构造/析构 =========================
SBTree::SBTree()
    : shortcut_(new SegmentedBlock()),
      data_head_(nullptr),
      data_tail_(nullptr),
      max_key_(std::numeric_limits<Key>::lowest())
{
    // 启动索引后台线程
    index_stop_.store(false, std::memory_order_relaxed);
    index_thread_ = std::thread(&SBTree::index_worker_, this);
}

SBTree::~SBTree()
{
    // 1) 刷新活跃段，转换并落盘到数据层
    flush();
    // 2) 等待索引层同步完成
    flush_index();
    // 3) 通知后台线程退出
    {
        std::lock_guard<std::mutex> lk(q_mu_);
        index_stop_.store(true, std::memory_order_release);
    }
    q_cv_.notify_all();
    if (index_thread_.joinable())
        index_thread_.join();

    // 4) 释放数据层链表
    DataBlock *cur = data_head_;
    while (cur)
    {
        DataBlock *nxt = cur->next();
        delete cur;
        cur = nxt;
    }
    data_head_ = data_tail_ = nullptr;
}

// ========================= 内部辅助 =========================
// 段转换 + 追加到数据层 + 入队索引任务
void SBTree::convert_and_append(SegmentedBlock *seg_to_convert)
{
    if (!seg_to_convert)
        return;
    std::vector<KVPair> sorted_data = seg_to_convert->collect_and_sort_data();
    delete seg_to_convert;
    if (sorted_data.empty())
        return;

    DataBlock *new_chain_head = nullptr;
    DataBlock *new_chain_tail = nullptr;
    std::vector<DataBlock *> new_blocks;
    new_blocks.reserve(16);

    const KVPair *current_pos = sorted_data.data();
    size_t remaining = sorted_data.size();
    bool first_block = true;
    Key prev_min = 0;

    while (remaining > 0)
    {
        DataBlock *new_block = new DataBlock();
        size_t consumed = new_block->build_from_sorted(current_pos, remaining);
        assert(consumed > 0);

        if (!new_chain_head)
            new_chain_head = new_chain_tail = new_block;
        else
        {
            new_chain_tail->set_next(new_block);
            new_chain_tail = new_block;
        }

        if (!first_block)
            assert(prev_min <= new_block->min_key());
        prev_min = new_block->min_key();
        first_block = false;
        new_blocks.push_back(new_block);

        current_pos += consumed;
        remaining -= consumed;
    }

    {
        std::lock_guard<std::mutex> g(data_layer_lock_);
        if (!data_tail_)
        {
            data_head_ = new_chain_head;
            data_tail_ = new_chain_tail;
        }
        else
        {
            data_tail_->set_next(new_chain_head);
            data_tail_ = new_chain_tail;
        }
        max_key_ = std::max<Key>(max_key_, sorted_data.back().key);
    }
    std::cout << "Appended " << sorted_data.size() << " entries to the data layer.\n";
    enqueue_index_task_(std::move(new_blocks));
}

// 入队索引任务
void SBTree::enqueue_index_task_(std::vector<DataBlock *> &&blocks)
{
    if (blocks.empty())
        return;
    idx_batches_enqueued_.fetch_add(1);
    idx_items_enqueued_.fetch_add(blocks.size());

    {
        std::lock_guard<std::mutex> lk(q_mu_);
        index_q_.emplace_back(std::move(blocks));
    }
    q_cv_.notify_one();
}

// 后台索引线程主循环
void SBTree::index_worker_()
{
    for (;;)
    {
        std::vector<DataBlock *> batch;
        {
            std::unique_lock<std::mutex> lk(q_mu_);
            q_cv_.wait(lk, [&]
                       { return index_stop_.load() || !index_q_.empty(); });
            if (index_stop_.load() && index_q_.empty())
                break;
            batch = std::move(index_q_.front());
            index_q_.pop_front();
            ++index_in_flight_;
        }
        {
            std::unique_lock<std::shared_mutex> wlock(search_mu_);
            search_.append_run(batch);
        }
        idx_batches_applied_.fetch_add(1);
        idx_items_applied_.fetch_add(batch.size());
        --index_in_flight_;
        q_cv_.notify_all();
    }
}

// ========================= 基本操作 =========================
// 刷新活跃段
void SBTree::flush()
{
    SegmentedBlock *final_seg = shortcut_.exchange(nullptr);
    if (final_seg)
        convert_and_append(final_seg);
}

// 等待索引完成
void SBTree::flush_index()
{
    std::unique_lock<std::mutex> lk(q_mu_);
    q_cv_.wait(lk, [&]
               { return index_q_.empty() && (index_in_flight_.load() == 0); });
}

// 插入（并发友好，支持段切换）
void SBTree::insert(Key key, Value value)
{
    // 仅当数据层已经有内容，且 key 小于当前最大 key，才视为延迟
    if (data_head_ && key <= max_key_)
    {
        insert_delayed(key, value);
        return;
    }

    for (;;)
    {
        SegmentedBlock *seg = shortcut_.load();
        if (seg && seg->append_ordered(key, value))
        {
            if (seg->should_seal())
            {
                auto *new_seg = new SegmentedBlock();
                SegmentedBlock *expected = seg;
                if (shortcut_.compare_exchange_strong(expected, new_seg))
                {
                    seg->seal();
                    convert_and_append(seg);
                }
                else
                {
                    delete new_seg;
                }
            }
            return;
        }
        auto *new_seg = new SegmentedBlock();
        SegmentedBlock *expected = seg;
        if (shortcut_.compare_exchange_strong(expected, new_seg))
        {
            if (seg)
            {
                seg->seal();
                convert_and_append(seg);
            }
            new_seg->append_ordered(key, value);
            return;
        }
        else
        {
            delete new_seg;
        }
    }
}

// 查找
bool SBTree::lookup(Key k, Value *out) const
{
    DataBlock *blk = find_candidate_(k);
    if (!blk)
        blk = data_head_;
    while (blk)
    {
        Value v{};
        if (blk->find(k, v))
        {
            if (out)
                *out = v;
            return true;
        }
        DataBlock *nxt = blk->next();
        if (!nxt || nxt->min_key() > k)
            break;
        blk = nxt;
    }
    return false;
}

// 扫描
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

// ========================= RangeCursor =========================
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
        const KVPair &e = blk_->get_entry(mid);
        if (e.key >= l_)
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
    const std::size_t n = blk_->size();
    while (idx_ < n)
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
    return next(out);
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
        blk = data_head_;
    return RangeCursor(this, l, r, blk);
}

// ========================= 验证/统计 =========================
bool SBTree::verify_data_layer(size_t expected_total_keys) const
{
    std::lock_guard<std::mutex> g(data_layer_lock_);
    size_t actual = 0;
    Key last = 0;
    DataBlock *cur = data_head_;
    while (cur)
    {
        for (size_t i = 0; i < cur->size(); ++i)
        {
            KVPair e = cur->get_entry(i);
            if (e.key != actual)
                return false;
            if (e.value != e.key * 10)
                return false;
            if (actual > 0 && e.key <= last)
                return false;
            last = e.key;
            ++actual;
        }
        cur = cur->next();
    }
    return actual == expected_total_keys;
}

DataBlock *SBTree::find_candidate_(Key k) const
{
    return search_.find_candidate(k);
}

void SBTree::insert_delayed(Key k, Value v)
{
    // 如果数据层还没建好，退回 fast path（极早期）
    if (!data_head_)
    {
        SegmentedBlock *seg = shortcut_.load();
        if (seg)
            seg->append_ordered(k, v);
        return;
    }

    std::vector<DataBlock *> new_blocks; // 可能产生的新块（split）

    {
        std::lock_guard<std::mutex> g(data_layer_lock_);

        // 在锁里复核 candidate（索引层可能滞后）
        DataBlock *blk = search_.find_candidate(k);
        if (!blk)
            blk = data_head_;
        while (blk->next() && blk->next()->min_key() <= k)
            blk = blk->next();

        // 块内有序插入；满块则 split 并重链
        if (!blk->insert_sorted(k, v))
        {
            DataBlock *right = blk->split();
            if (right)
            {
                right->set_next(blk->next());
                blk->set_next(right);
                new_blocks.push_back(right);
                // 依据 min_key 决定最终落入哪侧
                (k >= right->min_key() ? right : blk)->insert_sorted(k, v);
            }
            else
            {
                // 极端情况下（理论上不该发生），直接重试插入当前块
                blk->insert_sorted(k, v);
            }
        }
    } // 锁释放

    // 索引层入队放锁外，避免阻塞其他写入
    if (!new_blocks.empty())
        enqueue_index_task_(std::move(new_blocks));
}

uint64_t SBTree::index_batches_enqueued() const noexcept { return idx_batches_enqueued_.load(); }
uint64_t SBTree::index_batches_applied() const noexcept { return idx_batches_applied_.load(); }
uint64_t SBTree::index_items_enqueued() const noexcept { return idx_items_enqueued_.load(); }
uint64_t SBTree::index_items_applied() const noexcept { return idx_items_applied_.load(); }

std::size_t SBTree::index_levels() const { return search_.levels_snapshot(); }