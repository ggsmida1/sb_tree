#include <gtest/gtest.h>
#include <vector>
#include "SBTree.h"
#include "KVPair.h"

static std::vector<Value> seq_values(Key start_key, size_t n)
{
    std::vector<Value> v;
    v.reserve(n);
    for (Key k = start_key; k < start_key + static_cast<Key>(n); ++k)
        v.push_back(static_cast<Value>(k * 10));
    return v;
}

TEST(EndToEnd, EmptyTree)
{
    SBTree t;
    Value v{};
    EXPECT_FALSE(t.lookup(1, &v));
    EXPECT_FALSE(t.lookup(0, &v));

    std::vector<Value> out;
    EXPECT_EQ(t.scan_from(1, 5, out), 0u);
    EXPECT_TRUE(out.empty());

    out.clear();
    EXPECT_EQ(t.scan_range(10, 20, out), 0u);
    EXPECT_TRUE(out.empty());

    out.clear();
    EXPECT_EQ(t.scan_range(20, 10, out), 0u) << "l>r should return 0";
    EXPECT_TRUE(out.empty());
}

TEST(EndToEnd, OrderedInsertLookupScan)
{
    SBTree t;

    // 插入顺序键并 flush
    const Key N = 10000;
    for (Key i = 1; i <= N; ++i)
    {
        t.insert(i, static_cast<Value>(i * 10));
    }
    t.flush();

    // lookup 命中 / 未命中
    Value v{};
    EXPECT_TRUE(t.lookup(1, &v));
    EXPECT_EQ(v, 10);
    EXPECT_TRUE(t.lookup(N / 2, &v));
    EXPECT_EQ(v, (N / 2) * 10);
    EXPECT_TRUE(t.lookup(N, &v));
    EXPECT_EQ(v, N * 10);
    EXPECT_FALSE(t.lookup(0, &v));
    EXPECT_FALSE(t.lookup(N + 123, &v));

    // scan_from：单块内
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan_from(/*start=*/1, /*count=*/5, out), 5u);
        EXPECT_EQ(out, (std::vector<Value>{10, 20, 30, 40, 50}));
    }

    // scan_from：跨块（不依赖块容量，选中值段）
    {
        std::vector<Value> out;
        Key start = N / 2 - 2;
        size_t cnt = 10;
        EXPECT_EQ(t.scan_from(start, cnt, out), cnt);
        EXPECT_EQ(out, seq_values(start, cnt));
    }

    // scan_from：从最小值之前
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan_from(0, 3, out), 3u);
        EXPECT_EQ(out, (std::vector<Value>{10, 20, 30}));
    }

    // scan_from：尾部不足 count
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan_from(N - 3, 10, out), 4u);
        EXPECT_EQ(out, (std::vector<Value>{(N - 3) * 10, (N - 2) * 10, (N - 1) * 10, N * 10}));
    }

    // scan_range：覆盖末尾（右边界越界也能正确截断）
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan_range(9950, 10010, out), 51u);
        EXPECT_EQ(out, seq_values(9950, 51));
    }

    // scan_range：全外区间（小于全局最小）
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan_range(-100, 0, out), 0u);
        EXPECT_TRUE(out.empty());
    }

    // scan_range：全外区间（大于全局最大）
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan_range(N + 1, N + 1000, out), 0u);
        EXPECT_TRUE(out.empty());
    }

    // scan_range：逆区间（l>r）
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan_range(200, 199, out), 0u);
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
        t.flush(); // 多 run 的情况下构建搜索层
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

    // 区间扫描覆盖跨 run
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan_range(2995, 3005, out), 11u);
        EXPECT_EQ(out, seq_values(2995, 11));
    }
}
