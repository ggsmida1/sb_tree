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

size_t SBTree::scan_from(Key start, size_t count, std::vector<Value> &out) const
{
    if (count == 0)
        return 0;

    std::lock_guard<std::mutex> g(data_layer_lock_);

    // 1) 索引定位起点块；若索引为空/滞后，则退回链表头
    DataBlock *blk = search_.find_candidate(start); // 候选块（最后一个 min_key <= start）
    if (!blk)
        blk = data_head_; // 兜底：从链表头开始

    size_t remaining = count;
    Key cur = start;
    size_t taken_total = 0;

    while (blk && remaining > 0)
    {
        // 2) 在当前块从 cur 开始扫描，最多 remaining 个
        size_t got = blk->scan_from(cur, remaining, out); // 单块扫描（最多 remaining）
        taken_total += got;
        remaining -= got;

        if (remaining == 0)
            break;

        // 3) 右移到下一块，更新下一块的起始 key
        DataBlock *nxt = blk->next();
        if (!nxt)
            break;
        // 保持 cur 不变（全局起点）；可选：cur = std::max(cur, nxt->min_key());
        blk = nxt;
    }

    return taken_total;
}

size_t SBTree::scan_range(Key l, Key r, std::vector<Value> &out) const
{
    if (l > r)
        return 0;

    std::lock_guard<std::mutex> g(data_layer_lock_);

    // 1) 用索引定位起点块（最后一个 min_key <= l），否则从链表头开始
    DataBlock *blk = search_.find_candidate(l);
    if (!blk)
        blk = data_head_;

    size_t taken_total = 0;

    // 2) 跨块扫描，直到越过 r 或链表结束
    while (blk)
    {
        // 当前块最小键若已 > r，后面更不可能有
        if (blk->min_key() > r)
            break;

        taken_total += blk->scan_range(l, r, out);

        // 右移
        blk = blk->next();
        if (!blk)
            break;

        // 保持 l 不变即可；块内 scan_range 会再做二分裁剪
        // l = std::max(l, blk->min_key());  // 可选，等价
    }

    return taken_total;
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