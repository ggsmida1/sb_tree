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

// ========================= 2) DataBlock::split 行为验证 =========================
// 目的：split 后左右块均保持有序；min_key 严格递增；n-ary 索引已重建可查。
TEST(Internal, DataBlockSplit_RebuildsNAryAndKeepsOrder)
{
    DataBlock blk;

    // 插入一组严格递增的 key-value
    const int total = 16; // 取一个不太小的数，确保 split 生效
    for (int i = 0; i < total; ++i)
    {
        ASSERT_TRUE(blk.insert_sorted(i, i * 10));
    }
    ASSERT_EQ(blk.size(), static_cast<size_t>(total));
    ASSERT_EQ(blk.min_key(), 0u);

    // 触发分裂
    DataBlock *right = blk.split();
    ASSERT_NE(right, nullptr);

    // 左右块元素数相加等于原总数
    size_t left_count = blk.size();
    size_t right_count = right->size();
    EXPECT_EQ(left_count + right_count, static_cast<size_t>(total));

    // 左右块各自有序 & 左块最大键 < 右块最小键
    for (size_t i = 1; i < blk.size(); ++i)
        EXPECT_LT(blk.get_entry(i - 1).key, blk.get_entry(i).key);
    for (size_t i = 1; i < right->size(); ++i)
        EXPECT_LT(right->get_entry(i - 1).key, right->get_entry(i).key);
    EXPECT_LT(blk.get_entry(blk.size() - 1).key, right->get_entry(0).key);

    // n-ary 重建后，左右块都能快查自身元素
    for (size_t i = 0; i < blk.size(); ++i)
    {
        Value v{};
        EXPECT_TRUE(blk.find(blk.get_entry(i).key, v));
        EXPECT_EQ(v, blk.get_entry(i).value);
    }
    for (size_t i = 0; i < right->size(); ++i)
    {
        Value v{};
        EXPECT_TRUE(right->find(right->get_entry(i).key, v));
        EXPECT_EQ(v, right->get_entry(i).value);
    }

    delete right;
}

// ========================= 3) SearchLayer：分层构建与候选查找 =========================
// 目的：使用可控的 DataBlock 组（按 min_key 递增），以较小 fanout 触发多层晋升，
//       验证 find_candidate(k) 返回的叶子块指针与预期一致；levels_snapshot() >= 2。
TEST(Internal, SearchLayer_BuildsLevelsAndFindsCandidate)
{
    // 构造一批 DataBlock*，每块持有少量有序键，min_key 等差递增
    auto make_block = [](Key start, int count)
    {
        auto *b = new DataBlock();
        for (int i = 0; i < count; ++i)
        {
            // 插入严格递增的键，确保 min_key == start
            EXPECT_TRUE(b->insert_sorted(static_cast<Key>(start + i),
                                         static_cast<Value>((start + i) * 10)));
        }
        return b;
    };

    // 生成 16 个块：块 i 覆盖 [100*i, 100*i + 9]，min_key = 100*i
    std::vector<DataBlock *> blocks;
    blocks.reserve(16);
    for (int i = 0; i < 16; ++i)
        blocks.push_back(make_block(/*start=*/100 * i, /*count=*/10));

    // fanout 设小一点，确保能晋升出 2~3 层
    SearchLayer layer(/*fanout=*/4);

    // 以两批 append_run 的方式追加（每批 8 块），模拟多次 run 到达
    {
        std::vector<DataBlock *> batch1(blocks.begin(), blocks.begin() + 8);
        layer.append_run(batch1);
        EXPECT_GE(layer.levels_snapshot(), 1u);
    }
    {
        std::vector<DataBlock *> batch2(blocks.begin() + 8, blocks.end());
        layer.append_run(batch2);
        EXPECT_GE(layer.levels_snapshot(), 2u); // fanout=4，16 叶子通常能晋升出 2 层
    }

    // 针对若干代表性 key，验证 find_candidate() 命中预期块
    auto expect_hit = [&](Key k, int expected_block_idx)
    {
        DataBlock *cand = layer.find_candidate(k);
        ASSERT_NE(cand, nullptr) << "candidate not found for key=" << k;
        // 预期块的 min_key
        Key expected_min = blocks[expected_block_idx]->min_key();
        EXPECT_EQ(cand->min_key(), expected_min)
            << "key=" << k << " expected block idx=" << expected_block_idx;
    };

    // 命中块 0（[0..9] 映射到我们设置的 [0..9] + 100*i 的第 0 块其实是 [0..9]? 我们从 100*0=0 开始）
    expect_hit(/*k=*/0, /*expected_block_idx=*/0);
    expect_hit(/*k=*/5, /*expected_block_idx=*/0);
    // 命中块 3（[300..309]）
    expect_hit(/*k=*/303, /*expected_block_idx=*/3);
    // 命中块 7（[700..709]）
    expect_hit(/*k=*/705, /*expected_block_idx=*/7);
    // 命中块 8（[800..809]）
    expect_hit(/*k=*/809, /*expected_block_idx=*/8);
    // 命中块 15（[1500..1509]）
    expect_hit(/*k=*/1508, /*expected_block_idx=*/15);

    // 无符号 Key，-1 会转换成超大值 ⇒ 命中最后块
    EXPECT_EQ(layer.find_candidate(static_cast<Key>(-1)), blocks.back());

    // 释放我们创建的测试块（SearchLayer 只保留裸指针引用）
    for (auto *b : blocks)
        delete b;
}
