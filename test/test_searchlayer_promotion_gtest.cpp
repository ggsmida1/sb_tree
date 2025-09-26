// test/test_searchlayer_promotion_gtest.cpp
#include <gtest/gtest.h>
#include <vector>
#include "SearchLayer.h"
#include "DataBlock.h"
#include "KVPair.h"

TEST(SearchLayerPromotion, OnlyPromoteWhenFullF)
{
    const std::size_t F = 64; // 想测多少就填多少
    SearchLayer sl(F);

    std::vector<DataBlock *> owned;
    auto make_block_with_one = [&](Key k)
    {
        KVPair kv{k, static_cast<Value>(k * 10)};
        auto *b = new DataBlock();
        size_t consumed = b->build_from_sorted(&kv, 1);
        EXPECT_EQ(consumed, 1u);
        owned.push_back(b);
        return b;
    };
    auto append_one_leaf = [&](Key k)
    {
        std::vector<DataBlock *> run{make_block_with_one(k)};
        sl.append_run(run);
    };

    // 生成单调递增的 key
    Key next_k = 100;
    auto add_leaves = [&](std::size_t n)
    {
        for (std::size_t i = 0; i < n; ++i)
        {
            append_one_leaf(next_k);
            next_k += 100;
        }
    };

    // 初始只有叶层
    EXPECT_EQ(sl.levels(), 1u);

    // 加到 F-1 个叶子：仍然只有 1 层
    add_leaves(F - 1);
    EXPECT_EQ(sl.levels(), 1u);

    // 第 F 个叶子：出现 L1（levels=2）
    add_leaves(1);
    EXPECT_EQ(sl.levels(), 2u);

    // 再加到 (F^2 - 1) 个叶子：仍然只有 2 层
    add_leaves(F * F - F - 1);
    EXPECT_EQ(sl.levels(), 2u);

    // 第 F^2 个叶子：出现 L2（levels=3）
    add_leaves(1);
    EXPECT_EQ(sl.levels(), 3u);

    // 清理
    for (auto *p : owned)
        delete p;
}
