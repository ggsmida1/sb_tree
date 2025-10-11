#include "gtest/gtest.h"
#include "SBTree.h" // 引入你的 SBTree 头文件
#include <vector>
#include <numeric>
#include <algorithm>

// =============================================================================
// SBTreeTest Fixture
// =============================================================================
// 我们使用一个测试夹具（Test Fixture）来为一组测试提供共享的上下文。
// SetUp() 会在每个 TEST_F 之前运行，TearDown() 会在之后运行。
// 这样可以避免在每个测试用例中重复创建和销毁 SBTree 对象。
class SBTreeTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        tree = new SBTree();
    }

    void TearDown() override
    {
        delete tree;
        tree = nullptr;
    }

    // 辅助函数：插入一个连续的 KV 序列
    void insert_range(uint64_t start_key, size_t count)
    {
        for (size_t i = 0; i < count; ++i)
        {
            // 使用一个可预测的 value，方便后续验证
            tree->insert(start_key + i, (start_key + i) * 10);
        }
    }

    // 辅助函数：验证一个 key 范围的值
    void verify_range(uint64_t start_key, size_t count)
    {
        for (size_t i = 0; i < count; ++i)
        {
            Value val;
            Key k = start_key + i;
            ASSERT_TRUE(tree->lookup(k, &val)) << "Failed to find key: " << k;
            ASSERT_EQ(val, k * 10) << "Value mismatch for key: " << k;
        }
    }

    SBTree *tree;
};

// =============================================================================
// 测试用例
// =============================================================================

// 测试1：基本插入和查找
TEST_F(SBTreeTest, BasicInsertAndLookup)
{
    // 插入数据
    tree->insert(100, 1000);
    tree->insert(200, 2000);

    // 数据仍在 SegmentedBlock 中，尚未转换，所以 lookup 应该找不到
    Value val;
    ASSERT_FALSE(tree->lookup(100, &val));

    // 手动刷新，强制将 SegmentedBlock 转换为 DataBlock
    tree->flush();
    tree->flush_index(); // 等待索引更新

    // 现在应该能找到了
    ASSERT_TRUE(tree->lookup(100, &val));
    EXPECT_EQ(val, 1000);

    ASSERT_TRUE(tree->lookup(200, &val));
    EXPECT_EQ(val, 2000);

    // 查找一个不存在的 key
    ASSERT_FALSE(tree->lookup(999, &val));
}

// 测试2：Upsert（更新）语义测试
TEST_F(SBTreeTest, UpsertSemantics)
{
    tree->insert(10, 100);
    tree->insert(20, 200);
    tree->insert(10, 101); // 插入重复的 key，更新 value

    tree->flush();
    tree->flush_index();

    Value val;
    ASSERT_TRUE(tree->lookup(10, &val));
    EXPECT_EQ(val, 101); // 验证 value 是最新写入的

    ASSERT_TRUE(tree->lookup(20, &val));
    EXPECT_EQ(val, 200);
}

// 测试3：触发一次 SegmentedBlock 转换
// PerThreadDataBlock 容量约为 (16384 - 24) / 16 = 1022.5 -> 1022
// 我们插入 1100 个条目来确保 PTB 被写满并触发转换。
TEST_F(SBTreeTest, TriggerSingleConversion)
{
    const size_t num_inserts = 1100;
    insert_range(0, num_inserts);

    // 在单线程中，当 PTB 满了之后，下一次插入会触发转换
    tree->flush(); // 确保所有数据都已转换
    tree->flush_index();

    // 验证所有数据都可被查询
    verify_range(0, num_inserts);

    // 验证边界
    Value val;
    ASSERT_FALSE(tree->lookup(num_inserts, &val)); // 查不存在的 key
}

// 测试4：触发多次转换，构建多层索引
// DataBlock 容量约为 (4096 - 56 - 64) / 16 = 248
// 插入 20000 条数据，会创建约 20000/248 ≈ 81 个 DataBlock
TEST_F(SBTreeTest, TriggerMultipleConversions)
{
    const size_t num_inserts = 20000;
    insert_range(0, num_inserts);
    tree->flush();
    tree->flush_index();

    // 随机抽样检查
    verify_range(0, 10);
    verify_range(5000, 10);
    verify_range(19990, 10);

    // 确保 SearchLayer 已经建立
    ASSERT_GT(tree->index_levels(), 1);
}

// 测试5：简单范围扫描 (在一个 DataBlock 内)
TEST_F(SBTreeTest, SimpleScan)
{
    insert_range(0, 100);
    tree->flush();
    tree->flush_index();

    std::vector<Value> results;
    size_t count = tree->scan(10, 19, results); // [10, 19] 闭区间

    ASSERT_EQ(count, 10);
    ASSERT_EQ(results.size(), 10);
    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(results[i], (10 + i) * 10);
    }
}

// 测试6：跨 DataBlock 的范围扫描
TEST_F(SBTreeTest, CrossBlockScan)
{
    // 插入 1000 条，确保会产生多个 DataBlock (1000 / 248 ≈ 4+ blocks)
    const size_t num_inserts = 1000;
    insert_range(0, num_inserts);
    tree->flush();
    tree->flush_index();

    // 扫描范围跨越多个块的边界，例如从 240 到 260
    std::vector<Value> results;
    size_t count = tree->scan(240, 260, results);

    ASSERT_EQ(count, 21); // [240, 260] 包含 21 个 key
    ASSERT_EQ(results.size(), 21);
    for (size_t i = 0; i < 21; ++i)
    {
        EXPECT_EQ(results[i], (240 + i) * 10);
    }
}

// 测试7：范围扫描的边界条件
TEST_F(SBTreeTest, ScanEdgeCases)
{
    insert_range(100, 200); // Keys: [100, 299]
    tree->flush();
    tree->flush_index();

    std::vector<Value> results;
    // Case 1: 范围完全在所有数据之前
    EXPECT_EQ(tree->scan(0, 50, results), 0);
    EXPECT_TRUE(results.empty());

    // Case 2: 范围完全在所有数据之后
    EXPECT_EQ(tree->scan(300, 400, results), 0);
    EXPECT_TRUE(results.empty());

    // Case 3: 范围左边界与数据重合
    EXPECT_EQ(tree->scan(100, 105, results), 6);
    EXPECT_EQ(results.size(), 6);
    results.clear();

    // Case 4: 范围右边界与数据重合
    EXPECT_EQ(tree->scan(295, 299, results), 5);
    EXPECT_EQ(results.size(), 5);
    results.clear();

    // Case 5: 无效范围 l > r
    EXPECT_EQ(tree->scan(150, 140, results), 0);
    EXPECT_TRUE(results.empty());
}

// 测试8：RangeCursor 功能
TEST_F(SBTreeTest, RangeCursorTest)
{
    insert_range(0, 500);
    tree->flush();
    tree->flush_index();

    auto cursor = tree->open_range_cursor(240, 260);
    ASSERT_TRUE(cursor.valid());

    KVPair kv;
    size_t count = 0;
    while (cursor.next(&kv))
    {
        EXPECT_EQ(kv.key, 240 + count);
        EXPECT_EQ(kv.value, (240 + count) * 10);
        count++;
    }
    EXPECT_EQ(count, 21);
    ASSERT_FALSE(cursor.valid());
}