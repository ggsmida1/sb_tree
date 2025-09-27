#include "SBTree.h"
#include <vector>
#include <iostream>

SBTree::SBTree()
    : shortcut_(new SegmentedBlock()),
      data_head_(nullptr), // 初始化
      data_tail_(nullptr)  // 初始化
{
}

// 实现析构函数，遍历并删除所有 DataBlock
SBTree::~SBTree()
{
    // 确保在销毁前所有数据都被转换
    flush();

    // ... (删除 DataBlock 链表的代码) ...
    DataBlock *current = data_head_;
    while (current != nullptr)
    {
        DataBlock *next = current->next();
        delete current;
        current = next;
    }
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

    // 4) 搜索层：数据层尾插成功之后，再批量追加索引（保持“不领先”语义）
    search_.append_run(new_blocks);
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

void SBTree::insert(Key key, Value value)
{
    while (true)
    {
        SegmentedBlock *seg = shortcut_.load(std::memory_order_acquire);

        // 增加一个检查，如果 shortcut_ 在 flush 后变成 nullptr，就创建一个新的
        if (!seg)
        {
            seg = new SegmentedBlock();
            SegmentedBlock *expected = nullptr;
            // 尝试将新的 block 放入 shortcut_，如果失败说明已有其他线程放入
            if (!shortcut_.compare_exchange_strong(expected, seg))
            {
                delete seg; // 别的线程赢了，删除自己创建的
            }
            // 重新加载 seg 继续循环
            continue;
        }

        if (seg->append_ordered(key, value))
        {
            return;
        }

        SegmentedBlock *new_seg = new SegmentedBlock();
        SegmentedBlock *expected = seg;
        if (shortcut_.compare_exchange_weak(
                expected, new_seg,
                std::memory_order_acq_rel, std::memory_order_acquire))
        {
            // 转换逻辑现在由辅助函数完成
            convert_and_append(expected);

            if (!new_seg->append_ordered(key, value))
            { /* ... */
            }
            return;
        }
        else
        {
            delete new_seg;
        }
    }
}

bool SBTree::lookup(Key k, Value *out) const
{
    // 单线程阶段：读也加锁；以后可改为读优化
    std::lock_guard<std::mutex> g(data_layer_lock_);

    // 1) 先用搜索层定位候选块；若索引为空/滞后，则退回链表头
    DataBlock *blk = search_.find_candidate(k);
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

SBTree::RangeCursor SBTree::open_range_cursor(Key l, Key r) const
{
    if (l > r)
        return RangeCursor(this, 1, 0, nullptr); // 空区间
    DataBlock *blk = search_.find_candidate(l);
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