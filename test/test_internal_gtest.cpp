// test_internal_gtest.cpp
#include <gtest/gtest.h>
#include <vector>
#include <memory>
#include <algorithm>
#include "SBTree.h"
#include "DataBlock.h"
#include "SearchLayer.h"
#include "KVPair.h"

// ========================= 1) AutoDetect：填满即封段 =========================
// 目的：不设定魔数，自动探测第一次“段转换入队”的拐点，验证边界键已可见。
TEST(Internal, AutoDetect_SealOnFill)
{
    SBTree t;
    std::vector<uint64_t> out;

    // 基线：还没有任何转换任务入队
    uint64_t enq0 = t.index_batches_enqueued();

    // 逐个插入，直到第一次产生转换（有批次入队索引）
    uint64_t k = 0;
    for (;; ++k)
    {
        t.insert(k, k * 10);
        t.flush_index(); // 让 enqueued/applied 计数对测试可见
        if (t.index_batches_enqueued() > enq0)
        {
            break;
        }
    }

    // 断言：触发点 k 这一条已经可扫描到
    size_t got = t.scan(k, k, out);
    ASSERT_EQ(got, 1u);
    ASSERT_EQ(out[0], k * 10);

    // 断言：接缝左侧也在，连续两条都在数据层（说明 run 已成立）
    out.clear();
    got = t.scan(k - 1, k, out);
    ASSERT_EQ(got, 2u);
    ASSERT_EQ(out, (std::vector<uint64_t>{(k - 1) * 10, k * 10}));
}

TEST(Internal, SegmentAutoSealCreateNew)
{
    SBTree t;
    const size_t entries = 100000; // 足够触发多次转换
    for (size_t i = 0; i < entries; ++i)
        t.insert(i, i * 10);
    t.flush();
    t.flush_index();

    EXPECT_GT(t.index_batches_applied(), 1u);
}

TEST(Internal, DataBlockScanBoundary)
{
    DataBlock blk;
    std::vector<KVPair> kv;
    for (Key i = 0; i < 100; ++i)
        kv.push_back({i, i * 10});
    blk.build_from_sorted(kv.data(), kv.size());

    std::vector<Value> out;
    blk.scan_range(0, 99, out);
    ASSERT_EQ(out.size(), 100u);
    EXPECT_EQ(out.front(), 0u);
    EXPECT_EQ(out.back(), 990u);

    out.clear();
    blk.scan_range(50, 60, out);
    ASSERT_EQ(out.size(), 11u);
    EXPECT_EQ(out[0], 500u);
    EXPECT_EQ(out[10], 600u);
}

TEST(Internal, SearchLayerPromotionFind)
{
    SearchLayer layer(4); // 小扇出方便晋升
    std::vector<DataBlock *> blocks;
    for (int i = 0; i < 16; ++i)
    {
        DataBlock *b = new DataBlock();
        KVPair kv[10];
        for (int j = 0; j < 10; ++j)
            kv[j] = {static_cast<Key>(i * 10 + j), kv[j].key * 10};
        b->build_from_sorted(kv, 10);
        blocks.push_back(b);
    }
    layer.append_run(blocks);

    for (int i = 0; i < 160; ++i)
    {
        DataBlock *cand = layer.find_candidate(i);
        ASSERT_NE(cand, nullptr);
        EXPECT_LE(cand->min_key(), i);
    }

    for (auto b : blocks)
        delete b;
}
