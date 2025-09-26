#include <gtest/gtest.h>
#include <vector>
#include "SearchLayer.h"
#include "DataBlock.h"
#include "KVPair.h"

static DataBlock *make_block_with_one(Key k)
{
    KVPair kv{k, static_cast<Value>(k * 10)};
    auto *b = new DataBlock();
    size_t consumed = b->build_from_sorted(&kv, 1);
    EXPECT_EQ(consumed, 1u);
    return b;
}

TEST(SearchLayerTailUnpromoted, CandidateWorks)
{
    const std::size_t F = 8; // 也可改 64
    SearchLayer sl(F);

    std::vector<DataBlock *> owned;
    owned.reserve(F * 2);

    auto append_one_leaf = [&](Key k)
    {
        auto *b = make_block_with_one(k);
        owned.push_back(b);
        std::vector<DataBlock *> run{b};
        sl.append_run(run);
    };

    // 1) 先造出一组满 F 的叶子：100,200,...,F*100
    Key next_k = 100;
    for (std::size_t i = 0; i < F; ++i)
    {
        append_one_leaf(next_k);
        next_k += 100;
    }
    ASSERT_EQ(sl.levels(), 2u);

    // 2) 再追加一个不足 F 的尾巴
    const std::size_t tail_cnt = F / 2;
    std::vector<Key> tail_keys;
    tail_keys.reserve(tail_cnt);
    for (std::size_t i = 0; i < tail_cnt; ++i)
    {
        tail_keys.push_back(next_k);
        append_one_leaf(next_k);
        next_k += 100;
    }
    ASSERT_EQ(sl.levels(), 2u);

    // 3) 尾巴每个键：候选应存在，且其 min_key ≤ k，并且 ≥ F*100（最后一个已覆盖叶）
    Key last_covered = static_cast<Key>(F * 100);
    for (Key k : tail_keys)
    {
        DataBlock *cand = sl.find_candidate(k);
        ASSERT_NE(cand, nullptr);
        EXPECT_LE(cand->min_key(), k);
        EXPECT_GE(cand->min_key(), last_covered);
    }

    // 4) 对 “尾巴首键 - 1” ：候选应为最后一个已覆盖叶（F*100）
    Key first_tail = tail_keys.front();                       // e.g., 900
    DataBlock *cand_prev = sl.find_candidate(first_tail - 1); // 899
    ASSERT_NE(cand_prev, nullptr);
    EXPECT_EQ(cand_prev->min_key(), last_covered); // 800

    // 5) 小于全局最小：nullptr
    EXPECT_EQ(sl.find_candidate(42), nullptr);

    for (auto *p : owned)
        delete p; // 清理
}
