#include "SBTree.h"
#include <vector>
#include <iostream>

// ===== SBTree ctor/dtor =====
SBTree::SBTree()
    : shortcut_(new SegmentedBlock()),
      data_head_(nullptr),
      data_tail_(nullptr)
{
    // 启动索引专线线程
    index_stop_.store(false, std::memory_order_relaxed);
    index_thread_ = std::thread(&SBTree::index_worker_, this);
}

SBTree::~SBTree()
{
    // 1) 先把还在写的分段块转完并入队（数据层完成）
    flush();

    // 2) 等待索引层把已入队的批次都处理完（barrier）
    //    若你把 flush_index() 设为 private，也可以直接在这里调用。
    flush_index();

    // 3) 通知后台线程退出并 join
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

    // 5) 释放分段块
    delete shortcut_;
    shortcut_ = nullptr;
}

// ++ 新增：将转换逻辑提取为辅助函数 ++
void SBTree::convert_and_append(SegmentedBlock *seg_to_convert)
{
    if (!seg_to_convert)
        return;

    // 1) 收集并排序（单线程阶段在这里释放旧段是安全的）
    std::vector<KVPair> sorted_data = seg_to_convert->collect_and_sort_data();
    delete seg_to_convert;

    if (sorted_data.empty())
        return;

    // 2) 构建“本次 run”的 DataBlock 链，并记录这批块用于索引追加
    DataBlock *new_chain_head = nullptr;
    DataBlock *new_chain_tail = nullptr;
    std::vector<DataBlock *> new_blocks; // ← 关键：收集本次 run 的块
    new_blocks.reserve(16);              // 可选：粗略预留，避免多次扩容

    const KVPair *current_pos = sorted_data.data();
    size_t remaining = sorted_data.size();

    bool first_block = true;
    Key prev_min = 0;

    while (remaining > 0)
    {
        DataBlock *new_block = new DataBlock();
        size_t consumed = new_block->build_from_sorted(current_pos, remaining);
        assert(consumed > 0 && "build_from_sorted must consume > 0");

        // 串成临时链
        if (!new_chain_head)
        {
            new_chain_head = new_chain_tail = new_block;
        }
        else
        {
            new_chain_tail->set_next(new_block);
            new_chain_tail = new_block;
        }

        // 记录到本次 run 列表（要求按 min_key 非降）
        if (!first_block)
        {
            assert(prev_min <= new_block->min_key() && "blocks' min_key must be non-decreasing");
        }
        prev_min = new_block->min_key();
        first_block = false;
        new_blocks.push_back(new_block);

        current_pos += consumed;
        remaining -= consumed;
    }

    // 3) 追加到数据层（小临界区）
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
    }
    std::cout << "Appended " << sorted_data.size() << " entries to the data layer.\n";

    enqueue_index_task_(std::move(new_blocks));
}

void SBTree::enqueue_index_task_(std::vector<DataBlock *> &&blocks)
{
    if (blocks.empty())
        return;

    // 统计（入队）
    idx_batches_enqueued_.fetch_add(1, std::memory_order_relaxed);
    idx_items_enqueued_.fetch_add(blocks.size(), std::memory_order_relaxed);
    std::cout << "[index][enqueue] blocks=" << blocks.size()
              << " enq_batches=" << index_batches_enqueued()
              << " enq_items=" << index_items_enqueued()
              << " appl_batches=" << index_batches_applied()
              << " appl_items=" << index_items_applied() << "\n";

    {
        std::lock_guard<std::mutex> lk(q_mu_);
        index_q_.emplace_back(std::move(blocks));
    }
    q_cv_.notify_one();
}

void SBTree::index_worker_()
{
    for (;;)
    {
        std::vector<DataBlock *> batch;
        {
            std::unique_lock<std::mutex> lk(q_mu_);
            q_cv_.wait(lk, [&]
                       { return index_stop_.load(std::memory_order_acquire) || !index_q_.empty(); });
            if (index_stop_.load(std::memory_order_acquire) && index_q_.empty())
                break;
            batch = std::move(index_q_.front());
            index_q_.pop_front();
            ++index_in_flight_;
        }

        {
            std::unique_lock<std::shared_mutex> wlock(search_mu_);
            search_.append_run(batch);
        }

        // 统计（应用完成）
        idx_batches_applied_.fetch_add(1, std::memory_order_relaxed);
        idx_items_applied_.fetch_add(batch.size(), std::memory_order_relaxed);
        std::cout << "[index][applied] blocks=" << batch.size()
                  << " levels=" << index_levels()
                  << " enq_batches=" << index_batches_enqueued()
                  << " appl_batches=" << index_batches_applied() << "\n";

        --index_in_flight_;
        q_cv_.notify_all();
    }
}

// ++ 新增：实现 flush 方法 ++
void SBTree::flush()
{
    // 原子地取出当前的 shortcut_ 指针，并用 nullptr 替换它，
    // 这样就不会有新的写入操作进入即将被转换的块
    SegmentedBlock *final_seg = shortcut_.exchange(nullptr);

    // 如果 final_seg 不是空的，就对它进行转换
    if (final_seg)
    {
        std::cout << "Flushing final SegmentedBlock...\n";
        convert_and_append(final_seg);
    }
}

void SBTree::flush_index()
{
    std::unique_lock<std::mutex> lk(q_mu_);
    q_cv_.wait(lk, [&]
               { return index_q_.empty() && (index_in_flight_.load() == 0); });
}

void SBTree::insert(Key key, Value value)
{
    for (;;)
    {
        SegmentedBlock *seg = shortcut_.load(std::memory_order_acquire);

        // 1) 先尝试在当前活跃段写入
        if (seg && seg->append_ordered(key, value))
        {
            // 可选：维护 max_key_
            if (key > max_key_)
            {
                max_key_ = key;
            }

            // === 新增：如果刚被写满，则“立刻切段并转换旧段” ===
            if (seg->should_seal())
            {
                auto *new_seg = new SegmentedBlock();
                SegmentedBlock *expected = seg;
                if (shortcut_.compare_exchange_strong(expected, new_seg,
                                                      std::memory_order_acq_rel,
                                                      std::memory_order_acquire))
                {
                    // 我们赢了：封印旧段并转换
                    seg->seal();             // ACTIVE -> CONVERT（幂等）
                    convert_and_append(seg); // 收集→排序→切片→尾插数据层→索引入队
                    // 你的 convert_and_append 内会负责释放旧段（按你现有实现）
                }
                else
                {
                    // 别的线程已经切好了
                    delete new_seg;
                }
            }

            return; // 本次 key 已写成功（即使我们触发了切段也已经写完了）
        }

        // 2) 到这里说明 seg 不存在或拒写（非 ACTIVE / 槽满 / 已满）
        //    准备一个新段并尝试切换
        auto *new_seg = new SegmentedBlock();
        SegmentedBlock *expected = seg;
        if (shortcut_.compare_exchange_strong(expected, new_seg,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire))
        {
            // CAS 成功：封印旧段并转换（如有旧段）
            if (seg)
            {
                seg->seal();
                convert_and_append(seg);
            }
            // 把当前 key 写到新段里 —— 然后立刻返回，避免重复写
            bool ok = new_seg->append_ordered(key, value);
            (void)ok; // 或者 assert(ok);
            return;
        }
        else
        {
            delete new_seg;
        }

        // 回到 for(;;) 重试
    }
}

bool SBTree::lookup(Key k, Value *out) const
{
    // 单线程阶段：读也加锁；以后可改为读优化
    std::lock_guard<std::mutex> g(data_layer_lock_);

    // 1) 先用搜索层定位候选块；若索引为空/滞后，则退回链表头
    DataBlock *blk = find_candidate_locked_(k);
    if (!blk)
        blk = data_head_;

    // 2) 沿链表向右兜底（只靠 min_key 即可判断是否需要右移）
    while (blk)
    {
        Value v{};
        if (blk->find(k, v))

        { // 命中
            if (out)
                *out = v;
            return true;
        }
        DataBlock *nxt = blk->next();
        // 如果没有下一块，或下一块的 min_key 已经大于 k，说明后面不可能有 k
        if (!nxt || nxt->min_key() > k)
            break;
        blk = nxt; // 继续向右兜底
    }
    return false; // 未命中
}

size_t SBTree::scan(Key l, Key r, std::vector<Value> &out) const
{
    if (l > r)
        return 0;
    auto cur = open_range_cursor(l, r);
    size_t added = 0;

    // 这里演示“逐条拉取然后 push value”，也可以写成批量
    KVPair kv;
    while (cur.next(&kv))
    {
        out.push_back(kv.value);
        ++added;
    }
    return added;
}

// 之前是：加 shared_lock 再调 search_.find_candidate(k)
// 现在：SearchLayer::find_candidate 已经读无锁了，这里直接转发即可。
DataBlock *SBTree::find_candidate_locked_(Key k) const
{
    // no lock needed: SearchLayer uses snapshot (ROWEX) for readers
    return search_.find_candidate(k);
}

SBTree::RangeCursor SBTree::open_range_cursor(Key l, Key r) const
{
    if (l > r)
        return RangeCursor(this, 1, 0, nullptr); // 空区间
    DataBlock *blk = find_candidate_locked_(l);
    if (!blk)
        blk = data_head_;
    return RangeCursor(this, l, r, blk);
}

void SBTree::RangeCursor::seek_first_pos_()
{
    // 在当前块内二分到第一个 key >= l_
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

    // 若整块都 < l_，跳到下一块
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
        } // 超出右界，结束
        if (e.key >= l_)
        {
            if (out)
                *out = e;
            return true;
        } // 命中
        // e.key < l_：只会在首块发生，继续推进
    }

    // 跳到下一块
    blk_ = blk_->next();
    if (!blk_ || blk_->min_key() > r_)
    {
        blk_ = nullptr;
        return false;
    }
    idx_ = 0;
    return next(out); // 继续（尾递归）
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

bool SBTree::verify_data_layer(size_t expected_total_keys) const
{
    std::lock_guard<std::mutex> g(data_layer_lock_);
    std::cout << "\n--- Starting Verification ---\n";

    size_t actual_keys_count = 0;
    Key last_key = 0; // 假设 key 从 0 开始且不为 0

    DataBlock *current_block = data_head_;
    while (current_block != nullptr)
    {
        for (size_t i = 0; i < current_block->size(); ++i)
        {
            KVPair entry = current_block->get_entry(i);

            // 1. 验证 Key 的值是否等于我们期望的计数值
            if (entry.key != actual_keys_count)
            {
                std::cerr << "Verification FAILED: Key mismatch. Expected key "
                          << actual_keys_count << ", but got " << entry.key << ".\n";
                return false;
            }

            // 2. 验证 Value 是否符合我们插入时的规则 (value = key * 10)
            if (entry.value != entry.key * 10)
            {
                std::cerr << "Verification FAILED: Value mismatch for key " << entry.key
                          << ". Expected value " << entry.key * 10 << ", but got " << entry.value << ".\n";
                return false;
            }

            // 3. 验证数据是否全局有序
            if (actual_keys_count > 0 && entry.key <= last_key)
            {
                std::cerr << "Verification FAILED: Data is not sorted. Current key "
                          << entry.key << " is not greater than last key " << last_key << ".\n";
                return false;
            }

            last_key = entry.key;
            actual_keys_count++;
        }
        current_block = current_block->next();
    }

    // 4. 验证最终的总数是否匹配
    if (actual_keys_count != expected_total_keys)
    {
        std::cerr << "Verification FAILED: Total key count mismatch. Expected "
                  << expected_total_keys << ", but found " << actual_keys_count << ".\n";
        return false;
    }

    std::cout << "Verification PASSED: Found and verified " << actual_keys_count << " keys.\n";
    std::cout << "--- Verification Finished ---\n";
    return true;
}

uint64_t SBTree::index_batches_enqueued() const noexcept { return idx_batches_enqueued_.load(); }
uint64_t SBTree::index_batches_applied() const noexcept { return idx_batches_applied_.load(); }
uint64_t SBTree::index_items_enqueued() const noexcept { return idx_items_enqueued_.load(); }
uint64_t SBTree::index_items_applied() const noexcept { return idx_items_applied_.load(); }

std::size_t SBTree::index_levels() const
{
    return search_.levels_snapshot();
}
