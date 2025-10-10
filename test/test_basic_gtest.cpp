// test_basic_gtest.cpp
#include <gtest/gtest.h>
#include <vector>
#include <thread>
#include <chrono>
#include "SBTree.h"
#include "KVPair.h"

// 生成连续 value 序列（方便比较）
static std::vector<Value> seq_values(Key start_key, size_t n)
{
    std::vector<Value> v;
    v.reserve(n);
    for (Key k = start_key; k < start_key + static_cast<Key>(n); ++k)
        v.push_back(static_cast<Value>(k * 10));
    return v;
}

// ========================= 1. 空树 =========================
TEST(Basic, EmptyTree)
{
    SBTree t;
    Value v{};

    // lookup 空树
    EXPECT_FALSE(t.lookup(1, &v));
    EXPECT_FALSE(t.lookup(0, &v));

    // scan 空树
    std::vector<Value> out;
    EXPECT_EQ(t.scan(10, 20, out), 0u);
    EXPECT_TRUE(out.empty());

    // 逆区间空树
    out.clear();
    EXPECT_EQ(t.scan(20, 10, out), 0u);
    EXPECT_TRUE(out.empty());
}

// ========================= 2. 顺序插入（无延迟数据） =========================
TEST(Basic, OrderedInsertLookupScanAndCursor)
{
    SBTree t;

    const Key N = 5000;
    for (Key i = 1; i <= N; ++i)
        t.insert(i, static_cast<Value>(i * 10));
    t.flush(); // 转换所有活跃段
    t.flush_index();

    // lookup 检查
    Value v{};
    EXPECT_TRUE(t.lookup(1, &v));
    EXPECT_EQ(v, 10);
    EXPECT_TRUE(t.lookup(N / 2, &v));
    EXPECT_EQ(v, (N / 2) * 10);
    EXPECT_TRUE(t.lookup(N, &v));
    EXPECT_EQ(v, N * 10);
    EXPECT_FALSE(t.lookup(0, &v));
    EXPECT_FALSE(t.lookup(N + 1, &v));

    // scan 范围检查
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(1, 5, out), 5u);
        EXPECT_EQ(out, (std::vector<Value>{10, 20, 30, 40, 50}));
    }
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(4950, 5000, out), 51u);
        EXPECT_EQ(out, seq_values(4950, 51));
    }
}

TEST(Basic, VerifyAfterLargeInsert)
{
    SBTree t;
    const size_t N = 10000;
    for (size_t i = 0; i < N; ++i)
        t.insert(i, i * 10);
    t.flush();
    t.flush_index();
    EXPECT_TRUE(t.verify_data_layer(N));
}

// ========================= 4. 异步索引验证 =========================
TEST(Basic, AsyncIndexingBarrierCheck)
{
    SBTree t;
    const int runs = 4;
    const Key per_run = 3000;
    Key base = 1;

    // 产生多批 run，让后台线程有工作量
    for (int i = 0; i < runs; ++i)
    {
        for (Key k = base; k < base + per_run; ++k)
            t.insert(k, static_cast<Value>(k * 10));
        t.flush(); // 数据层完成，但索引层异步
                   // 确认确实存在异步积压（有任务未完成）
        base += per_run;
    }

    const Key last = base - 1;

    // 未等待索引完成时的查找
    Value v{};
    EXPECT_TRUE(t.lookup(1, &v));
    EXPECT_EQ(v, 10);
    EXPECT_TRUE(t.lookup(last, &v));
    EXPECT_EQ(v, last * 10);
    EXPECT_FALSE(t.lookup(base, &v));

    // 强制索引追平后结果仍相同
    t.flush_index();
    EXPECT_TRUE(t.lookup(1, &v));
    EXPECT_EQ(v, 10);
    EXPECT_TRUE(t.lookup(last, &v));
    EXPECT_EQ(v, last * 10);
    EXPECT_FALSE(t.lookup(base, &v));
}