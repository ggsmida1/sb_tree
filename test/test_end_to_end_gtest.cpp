// test/test_end_to_end_gtest.cpp
#include <gtest/gtest.h>
#include <vector>
#include "SBTree.h"
#include "KVPair.h"

static std::vector<Value> seq_values(Key start_key, size_t n)
{
    std::vector<Value> v;
    v.reserve(n);
    for (Key k = start_key; k < start_key + static_cast<Key>(n); ++k)
    {
        v.push_back(static_cast<Value>(k * 10));
    }
    return v;
}

TEST(EndToEnd, EmptyTree)
{
    SBTree t;
    Value v{};

    // lookup
    EXPECT_FALSE(t.lookup(1, &v));
    EXPECT_FALSE(t.lookup(0, &v));

    // scan: 空树返回 0
    std::vector<Value> out;
    EXPECT_EQ(t.scan(10, 20, out), 0u);
    EXPECT_TRUE(out.empty());

    // 逆区间
    out.clear();
    EXPECT_EQ(t.scan(20, 10, out), 0u);
    EXPECT_TRUE(out.empty());
}

TEST(EndToEnd, OrderedInsertLookupScan)
{
    SBTree t;

    // 顺序插入并 flush
    const Key N = 10000;
    for (Key i = 1; i <= N; ++i)
    {
        t.insert(i, static_cast<Value>(i * 10));
    }
    t.flush();

    // lookup：命中/未命中
    Value v{};
    EXPECT_TRUE(t.lookup(1, &v));
    EXPECT_EQ(v, 10);
    EXPECT_TRUE(t.lookup(N / 2, &v));
    EXPECT_EQ(v, (N / 2) * 10);
    EXPECT_TRUE(t.lookup(N, &v));
    EXPECT_EQ(v, N * 10);
    EXPECT_FALSE(t.lookup(0, &v));
    EXPECT_FALSE(t.lookup(N + 123, &v));

    // scan：单块内（1..5）
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(/*l=*/1, /*r=*/5, out), 5u);
        EXPECT_EQ(out, (std::vector<Value>{10, 20, 30, 40, 50}));
    }

    // scan：跨块（选择一个中段，长度=10）
    {
        std::vector<Value> out;
        Key l = N / 2 - 2;
        Key r = l + 9; // 共10个
        EXPECT_EQ(t.scan(l, r, out), 10u);
        EXPECT_EQ(out, seq_values(l, 10));
    }

    // scan：从最小值之前开始（0..3 → 实际产出 1..3）
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(0, 3, out), 3u);
        EXPECT_EQ(out, (std::vector<Value>{10, 20, 30}));
    }

    // scan：尾部不足（N-3..N → 4个）
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(N - 3, N, out), 4u);
        EXPECT_EQ(out, (std::vector<Value>{(N - 3) * 10, (N - 2) * 10, (N - 1) * 10, N * 10}));
    }

    // scan：覆盖末尾（9950..10010 → 9950..10000 共 51）
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(9950, 10010, out), 51u);
        EXPECT_EQ(out, seq_values(9950, 51));
    }

    // 全外区间（小于全局最小）
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(-100, 0, out), 0u);
        EXPECT_TRUE(out.empty());
    }

    // 全外区间（大于全局最大）
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(N + 1, N + 1000, out), 0u);
        EXPECT_TRUE(out.empty());
    }

    // 逆区间（l>r）
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(200, 199, out), 0u);
        EXPECT_TRUE(out.empty());
    }
}

TEST(EndToEnd, MultipleRunsStillCorrect)
{
    SBTree t;

    auto insert_batch = [&](Key a, Key b)
    {
        for (Key k = a; k <= b; ++k)
            t.insert(k, static_cast<Value>(k * 10));
        t.flush();
    };

    insert_batch(1, 3000);
    insert_batch(3001, 6000);
    insert_batch(6001, 10000);

    // 点查抽样
    Value v{};
    EXPECT_TRUE(t.lookup(1, &v));
    EXPECT_EQ(v, 10);
    EXPECT_TRUE(t.lookup(5000, &v));
    EXPECT_EQ(v, 50000);
    EXPECT_TRUE(t.lookup(10000, &v));
    EXPECT_EQ(v, 100000);

    // 区间扫跨 run
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(2995, 3005, out), 11u);
        EXPECT_EQ(out, seq_values(2995, 11));
    }
}
