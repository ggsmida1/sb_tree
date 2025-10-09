#include <gtest/gtest.h>
#include "SBTree.h"

TEST(DelayedInsert, InsertOutOfOrderMaintainsOrder)
{
    SBTree t;

    // Step 1: 正常顺序插入 [100..200]
    for (uint64_t k = 100; k <= 200; ++k)
        t.insert(k, k * 10);

    // Step 2: 插入延迟数据 [50..99]，全部比当前 max_key_ 小
    for (uint64_t k = 50; k < 100; ++k)
        t.insert(k, k * 10);

    // Step 3: 再插入新数据 [201..220]
    for (uint64_t k = 201; k <= 220; ++k)
        t.insert(k, k * 10);

    // flush + build index
    t.flush();
    t.flush_index();

    // Step 4: 全范围扫描
    std::vector<uint64_t> out;
    size_t got = t.scan(50, 220, out);

    ASSERT_EQ(got, static_cast<size_t>(171)); // 50..220 共 171 个键
    // 验证全局单调递增且值正确
    for (size_t i = 0; i < out.size(); ++i)
    {
        ASSERT_EQ(out[i], static_cast<uint64_t>((50 + i) * 10))
            << "Mismatch at key " << (50 + i);
    }
}

TEST(DataBlockSplit, SplitKeepsOrderAndRebuildsIndex)
{
    // 创建一个 DataBlock，并假设它的容量为 8（可根据实际 DataBlock::kCapacity 调整）
    DataBlock blk;

    // 插入一组严格递增的 key-value
    const int total = 8;
    for (int i = 0; i < total; ++i)
    {
        blk.insert_sorted(i, i * 10);
    }

    ASSERT_EQ(blk.size(), static_cast<size_t>(total));

    EXPECT_EQ(blk.min_key(), 0u) << "min_key_ should be updated by insert_sorted()";

    // 手动触发分裂
    DataBlock *right = blk.split();

    // ===== 左右块元素数量 =====
    size_t left_count = blk.size();
    size_t right_count = right->size();
    EXPECT_EQ(left_count + right_count, static_cast<size_t>(total));

    // ===== 左右块有序性 =====
    EXPECT_LT(blk.min_key(), right->min_key()) << "右块 min_key 应大于左块";

    // ===== 左右块内容正确 =====
    // 验证左块内递增
    for (size_t i = 1; i < blk.size(); ++i)
    {
        EXPECT_LT(blk.get_entry(i - 1).key, blk.get_entry(i).key);
    }
    // 验证右块内递增
    for (size_t i = 1; i < right->size(); ++i)
    {
        EXPECT_LT(right->get_entry(i - 1).key, right->get_entry(i).key);
    }
    // 验证左块最大 key < 右块最小 key
    EXPECT_LT(blk.get_entry(blk.size() - 1).key, right->get_entry(0).key);

    // ===== N-Ary 索引重建 =====
    // 这里假设 DataBlock::find() 使用 n-ary 索引
    // 我们验证左右块都能 find 自己的元素
    for (size_t i = 0; i < blk.size(); ++i)
    {
        Value v;
        EXPECT_TRUE(blk.find(blk.get_entry(i).key, v));
        EXPECT_EQ(v, blk.get_entry(i).value);
    }
    for (size_t i = 0; i < right->size(); ++i)
    {
        Value v;
        EXPECT_TRUE(right->find(right->get_entry(i).key, v));
        EXPECT_EQ(v, right->get_entry(i).value);
    }

    // ===== 输出调试信息 =====
    std::cout << "[SplitTest] left_count=" << left_count
              << " right_count=" << right_count
              << " left_min=" << blk.min_key()
              << " right_min=" << right->min_key() << std::endl;

    // 清理
    delete right;
}

#include <thread>
#include <atomic>

TEST(DelayedInsert, InterleavedThreadsMaintainOrder)
{
    SBTree t;
    const size_t num_threads = 4;
    const size_t per_thread_keys = 250; // 顺序阶段目标总键 ~ 800
    // ---- 顺序阶段：交错写 0..799，但跳过 key%5==2 的键（给延迟阶段用）----
    for (size_t tid = 0; tid < num_threads; ++tid)
    {
        std::thread([&, tid]()
                    {
            for (size_t i = 0; i < per_thread_keys - 50; ++i) {
                uint64_t k = i * num_threads + tid; // 0..799 交错
                if (k % 5 == 2) continue;           // 预留空洞
                t.insert(k, k * 10);
            } })
            .join(); // 你也可以保留并发，这里为了示例简单串行
    }

    // ---- 延迟阶段：只写之前跳过的 “空洞键”，这些键 < max_key_，但不重复 ----
    for (size_t tid = 0; tid < num_threads; ++tid)
    {
        std::thread([&, tid]()
                    {
            for (uint64_t k = tid; k < 800; k += num_threads) {
                if (k % 5 == 2) {                    // 填空洞
                    t.insert(k, k * 10);            // 延迟路径触发
                }
            } })
            .join();
    }

    t.flush();
    t.flush_index();

    // 验证严格递增 & 值映射
    std::vector<uint64_t> out;
    size_t got = t.scan(0, 799, out);
    ASSERT_EQ(got, 800u);
    for (size_t i = 1; i < out.size(); ++i)
    {
        ASSERT_LT(out[i - 1], out[i]); // 严格递增
    }
    for (size_t i = 0; i < out.size(); ++i)
    {
        EXPECT_EQ(out[i], static_cast<uint64_t>(i * 10));
    }
}
