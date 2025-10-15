#include "SBTree.h"
#include <vector>
#include <iostream>

// ========================= 构造/析构 =========================
SBTree::SBTree()
    : shortcut_(new SegmentedBlock()),
      data_head_(nullptr),
      data_tail_(nullptr)
{
    // 启动索引后台线程
    index_stop_.store(false, std::memory_order_relaxed);
    index_thread_ = std::thread(&SBTree::index_worker_, this);
}

SBTree::~SBTree()
{
    // 1. 先停后台线程
    {
        std::lock_guard<std::mutex> lk(q_mu_);
        index_stop_.store(true, std::memory_order_release);
    }
    q_cv_.notify_all();

    // 2. 等待任务完成
    flush();
    flush_index();

    // 3. 等待线程退出
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

    // 5) 释放活跃分段块
    delete shortcut_.load();
    shortcut_ = nullptr;
}

// ========================= 内部辅助 =========================

// 后台索引线程主循环
void SBTree::index_worker_()
{
    for (;;)
    {
        SegmentedBlock *seg_to_convert = nullptr;
        {
            std::unique_lock<std::mutex> lk(q_mu_);
            q_cv_.wait(lk, [&]
                       { return index_stop_.load() || !segments_to_convert_q_.empty(); });

            if (index_stop_.load() && segments_to_convert_q_.empty())
                break;

            seg_to_convert = segments_to_convert_q_.front();
            segments_to_convert_q_.pop_front();
            ++index_in_flight_;
        }

        std::vector<KVPair> sorted_data = seg_to_convert->collect_and_sort_data();
        delete seg_to_convert;
        if (sorted_data.empty())
        {
            --index_in_flight_;
            q_cv_.notify_all();
            continue;
        }

        std::vector<DataBlock *> new_blocks;
        DataBlock *new_chain_head = nullptr;
        DataBlock *new_chain_tail = nullptr;
        const KVPair *current_pos = sorted_data.data();
        size_t remaining = sorted_data.size();
        while (remaining > 0)
        {
            auto *new_block = new DataBlock();
            size_t consumed = new_block->build_from_sorted(current_pos, remaining);
            assert(consumed > 0);

            if (!new_chain_head)
                new_chain_head = new_chain_tail = new_block;
            else
            {
                new_chain_tail->set_next(new_block);
                new_chain_tail = new_block;
            }
            new_blocks.push_back(new_block);
            current_pos += consumed;
            remaining -= consumed;
        }

        // 3. 追加到主数据层链表
        {
            std::lock_guard<std::mutex> g(data_layer_lock_);

            if (data_tail_ != nullptr && !new_blocks.empty())
            {
                Key old_max = data_tail_->max_key();
                Key new_min = new_blocks.front()->min_key();
                // fprintf(stderr, "[ASSERT_CHECK] old_max_key=%llu, new_min_key=%llu\n",
                //         (unsigned long long)old_max, (unsigned long long)new_min);
                assert(new_min > old_max && "FATAL INVARIANT VIOLATION: Key ranges overlap!");
            }

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
        }

        // 4. 应用到搜索层
        {
            std::unique_lock<std::shared_mutex> wlock(search_mu_);
            search_.append_run(new_blocks);
        }

        {
            std::lock_guard<std::mutex> lk(q_mu_);
            --index_in_flight_;
            q_cv_.notify_all();
        }
    }
}
// ========================= 基本操作 =========================
// 刷新活跃段
void SBTree::flush()
{
    SegmentedBlock *final_seg = shortcut_.exchange(nullptr);
    if (!final_seg)
    {
        return;
    }

    bool empty = final_seg->is_completely_empty(); // 如果你还没实现，就暂时假设 false

    if (empty)
    {
        delete final_seg;
        return;
    }

    final_seg->seal();
    {
        std::lock_guard<std::mutex> lk(q_mu_);
        segments_to_convert_q_.push_back(final_seg);
    }
    q_cv_.notify_one();
}

// 等待索引完成
void SBTree::flush_index()
{
    // fprintf(stderr, "[flush_index] waiting for background queue to drain...\n");
    std::unique_lock<std::mutex> lk(q_mu_);
    q_cv_.wait(lk, [&]
               {
        bool done = segments_to_convert_q_.empty() &&
                    (index_in_flight_.load() == 0);
        // if (!done) {
        //     fprintf(stderr, "[flush_index] still pending: queue=%zu, in_flight=%d\n",
        //             segments_to_convert_q_.size(),
        //             (int)index_in_flight_.load());
        // }
        return done; });
    // fprintf(stderr, "[flush_index] all segments converted.\n");
}

// 插入（并发友好，支持段切换）
void SBTree::insert(Key key, Value value)
{
    for (;;) // 这个重试循环处理所有竞争情况
    {
        SegmentedBlock *seg = shortcut_.load(std::memory_order_acquire);

        // --- (1) 若当前活跃段为空，执行自举安装 ---
        if (seg == nullptr)
        {

            // 尝试创建新的活跃段（bootstrap）
            auto *new_seg = new SegmentedBlock();
            SegmentedBlock *expected = nullptr;

            // CAS 尝试安装新段（只有一个线程能成功）
            if (shortcut_.compare_exchange_strong(expected, new_seg,
                                                  std::memory_order_release,
                                                  std::memory_order_relaxed))
            {
                // 我们成功安装了新段，下一轮循环即可正常插入
                continue;
            }
            else
            {
                // 有其他线程抢先安装了段，丢弃我们刚创建的
                delete new_seg;
                // 下一轮循环使用别人安装的段
                continue;
            }
        }

        // --- (2) 正常插入路径 ---
        if (seg->append_ordered(key, value))
        {
            // 键已成功插入当前段。

            // 原子性地更新 max_key
            Key current_max = max_key_.load(std::memory_order_relaxed);
            while (key > current_max)
            {
                if (max_key_.compare_exchange_weak(current_max, key, std::memory_order_relaxed))
                    break;
            }

            // 检查是否需要切段
            if (seg->should_seal())
            {
                // 本次插入刚好填满了段。
                auto *new_seg = new SegmentedBlock();
                SegmentedBlock *expected = seg;

                if (shortcut_.compare_exchange_strong(expected, new_seg,
                                                      std::memory_order_release,
                                                      std::memory_order_relaxed))
                {
                    // CAS 成功：当前线程负责封段 + 入队
                    seg->seal();
                    {
                        std::lock_guard<std::mutex> lk(q_mu_);
                        segments_to_convert_q_.push_back(seg);
                    }
                    q_cv_.notify_one();
                }
                else
                {
                    // CAS 失败：别人已经替换了，我们只需清理
                    delete new_seg;
                }
            }
            return; // 插入成功
        }

        // --- (3) 失败/重试路径 ---
        // append_ordered 失败或段被封印，尝试切换新段
        SegmentedBlock *current_seg_on_failure = shortcut_.load(std::memory_order_acquire);
        if (current_seg_on_failure) // 仅当确实有旧段需要替换时才尝试
        {
            auto *new_seg = new SegmentedBlock();
            SegmentedBlock *expected = current_seg_on_failure;

            if (shortcut_.compare_exchange_strong(expected, new_seg,
                                                  std::memory_order_release,
                                                  std::memory_order_relaxed))
            {
                current_seg_on_failure->seal();
                {
                    std::lock_guard<std::mutex> lk(q_mu_);
                    segments_to_convert_q_.push_back(current_seg_on_failure);
                }
                q_cv_.notify_one();
            }
            else
            {
                delete new_seg; // 别的线程赢了
            }
        }
        // 循环重试
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

std::size_t SBTree::index_levels() const { return search_.levels_snapshot(); }