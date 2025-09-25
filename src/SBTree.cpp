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

    std::vector<KVPair> sorted_data = seg_to_convert->collect_and_sort_data();
    delete seg_to_convert; // 在这里释放旧块

    if (!sorted_data.empty())
    {
        // ... (分块并追加到主链表的逻辑，和之前一样) ...
        DataBlock *new_chain_head = nullptr;
        DataBlock *new_chain_tail = nullptr;
        const KVPair *current_pos = sorted_data.data();
        size_t remaining = sorted_data.size();
        while (remaining > 0)
        {
            DataBlock *new_block = new DataBlock();
            size_t consumed = new_block->build_from_sorted(current_pos, remaining);
            if (!new_chain_head)
            {
                new_chain_head = new_chain_tail = new_block;
            }
            else
            {
                new_chain_tail->set_next(new_block);
                new_chain_tail = new_block;
            }
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
        }
        std::cout << "Appended " << sorted_data.size() << " entries to the data layer.\n";
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