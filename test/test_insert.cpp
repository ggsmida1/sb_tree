#include <iostream>
#include "SBTree.h"

int main()
{
    std::cout << "SB-Tree Sequential Insert Test\n";
    std::cout << "==============================\n";

    // 1. 创建 SBTree 实例
    SBTree tree;

    // 2. 定义插入总数
    // 一个 SegmentedBlock 大约能容纳 128 * 255 = 32640 条记录
    // 插入 10 万条数据，足以触发至少 3 次转换过程
    const size_t TOTAL_KEYS_TO_INSERT = 100000;
    std::cout << "Preparing to insert " << TOTAL_KEYS_TO_INSERT << " sequential keys...\n";

    // 3. 执行顺序插入
    for (size_t i = 0; i < TOTAL_KEYS_TO_INSERT; ++i)
    {
        // 插入的 key 是 i，value 是 i * 10，方便后续验证
        tree.insert(static_cast<Key>(i), static_cast<Value>(i * 10));
    }
    std::cout << "Insertion phase complete.\n";

    // ++ 新增：在验证前，强制 flush 所有剩余数据 ++
    tree.flush();

    // 4. 执行验证
    // 调用我们新增的验证方法，检查数据是否都已正确、有序地存入 DataBlock
    bool success = tree.verify_data_layer(TOTAL_KEYS_TO_INSERT);

    std::cout << "\n==============================\n";
    if (success)
    {
        std::cout << "Test Result: SUCCESS\n";
    }
    else
    {
        std::cout << "Test Result: FAILED\n";
    }

    return 0;
}

// SBTree 的析构函数会在 main 函数结束时自动调用，释放所有内存