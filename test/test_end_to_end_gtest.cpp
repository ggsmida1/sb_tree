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

static std::vector<Value> collect_values_from_pairs(const std::vector<KVPair> &kvs)
{
    std::vector<Value> out;
    out.reserve(kvs.size());
    for (auto &e : kvs)
        out.push_back(e.value);
    return out;
}

TEST(EndToEnd, EmptyTree)
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

    // 游标：空/逆区间立即耗尽
    {
        auto cur = t.open_range_cursor(10, 9);
        KVPair kv;
        EXPECT_FALSE(cur.next(&kv));
        std::vector<KVPair> buf;
        EXPECT_EQ(cur.next_batch(buf, 8), 0u);
    }
}

TEST(EndToEnd, OrderedInsertLookupScanAndCursor)
{
    SBTree t;

    // 顺序插入并 flush
    const Key N = 10000;
    for (Key i = 1; i <= N; ++i)
        t.insert(i, static_cast<Value>(i * 10));
    t.flush();

    // === lookup：命中/未命中 ===
    Value v{};
    EXPECT_TRUE(t.lookup(1, &v));
    EXPECT_EQ(v, 10);
    EXPECT_TRUE(t.lookup(N / 2, &v));
    EXPECT_EQ(v, (N / 2) * 10);
    EXPECT_TRUE(t.lookup(N, &v));
    EXPECT_EQ(v, N * 10);
    EXPECT_FALSE(t.lookup(0, &v));
    EXPECT_FALSE(t.lookup(N + 123, &v));

    // === scan：单块内（1..5） ===
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(/*l=*/1, /*r=*/5, out), 5u);
        EXPECT_EQ(out, (std::vector<Value>{10, 20, 30, 40, 50}));
    }

    // === scan：跨块中段（长度=10） ===
    {
        std::vector<Value> out;
        Key l = N / 2 - 2, r = l + 9; // 共10个
        EXPECT_EQ(t.scan(l, r, out), 10u);
        EXPECT_EQ(out, seq_values(l, 10));
    }

    // === scan：从最小值之前（0..3 → 实际 1..3） ===
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(0, 3, out), 3u);
        EXPECT_EQ(out, (std::vector<Value>{10, 20, 30}));
    }

    // === scan：尾部不足（N-3..N → 4 个） ===
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(N - 3, N, out), 4u);
        EXPECT_EQ(out, (std::vector<Value>{(N - 3) * 10, (N - 2) * 10, (N - 1) * 10, N * 10}));
    }

    // === scan：覆盖末尾（9950..10010 → 9950..10000 共 51） ===
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(9950, 10010, out), 51u);
        EXPECT_EQ(out, seq_values(9950, 51));
    }

    // === 边界键：游标只产出 1 条 ===
    {
        // [1,1]
        auto c1 = t.open_range_cursor(1, 1);
        KVPair kv;
        ASSERT_TRUE(c1.next(&kv));
        EXPECT_EQ(kv.key, 1);
        EXPECT_EQ(kv.value, 10);
        EXPECT_FALSE(c1.next(&kv)); // 耗尽

        // [N,N]
        auto c2 = t.open_range_cursor(N, N);
        ASSERT_TRUE(c2.next(&kv));
        EXPECT_EQ(kv.key, N);
        EXPECT_EQ(kv.value, N * 10);
        EXPECT_FALSE(c2.next(&kv));
    }

    // === 游标跨块严格截断（9950..10010） ===
    {
        auto cur = t.open_range_cursor(9950, 10010);
        KVPair kv;
        std::vector<KVPair> got;
        while (cur.next(&kv))
            got.push_back(kv);
        ASSERT_FALSE(cur.next(&kv)); // 再次 next 仍为 false
        ASSERT_FALSE(got.empty());
        EXPECT_EQ(got.front().key, 9950);
        EXPECT_LE(got.back().key, 10000); // 严格 ≤ r 截断
        EXPECT_EQ(collect_values_from_pairs(got), seq_values(9950, 51));
    }

    // === 批量拉取 next_batch：多次小批拼起来 ===
    {
        auto cur = t.open_range_cursor(9950, 10010);
        std::vector<KVPair> buf;
        buf.reserve(8);
        std::vector<KVPair> all;
        size_t total = 0;
        for (;;)
        {
            buf.clear();
            size_t n = cur.next_batch(buf, /*limit=*/7); // 小批次
            if (n == 0)
                break;
            total += n;
            all.insert(all.end(), buf.begin(), buf.end());
        }
        EXPECT_EQ(total, 51u);
        EXPECT_EQ(collect_values_from_pairs(all), seq_values(9950, 51));
        // 再拉一次应为 0（耗尽语义）
        buf.clear();
        EXPECT_EQ(cur.next_batch(buf, 7), 0u);
    }

    // === 全外区间（小于最小 & 大于最大） ===
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(-100, 0, out), 0u);
        EXPECT_TRUE(out.empty());
        EXPECT_EQ(t.scan(N + 1, N + 1000, out), 0u);
        EXPECT_TRUE(out.empty());
    }

    // === 逆区间（l>r） ===
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

    // 区间扫跨 run（接缝处连续、无重无漏）
    {
        std::vector<Value> out;
        EXPECT_EQ(t.scan(2995, 3005, out), 11u);
        EXPECT_EQ(out, seq_values(2995, 11));
    }

    // 游标在接缝处同样正确
    {
        auto cur = t.open_range_cursor(2995, 3005);
        KVPair kv;
        std::vector<KVPair> got;
        while (cur.next(&kv))
            got.push_back(kv);
        EXPECT_EQ(collect_values_from_pairs(got), seq_values(2995, 11));
    }
}
