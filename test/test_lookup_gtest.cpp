#include <gtest/gtest.h>
#include "SBTree.h"
#include "KVPair.h"

TEST(SBTreeLookup, EndToEndOrderedKeys)
{
    SBTree t;

    const Key N = 20000;
    for (Key i = 1; i <= N; ++i)
    {
        t.insert(i, static_cast<Value>(i * 10));
    }
    t.flush(); // 触发转换→数据层尾插→搜索层 append_run

    auto hit = [&](Key k)
    {
        Value v{};
        ASSERT_TRUE(t.lookup(k, &v)) << "miss at key=" << k;
        EXPECT_EQ(v, k * 10);
    };
    auto miss = [&](Key k)
    {
        Value v{};
        ASSERT_FALSE(t.lookup(k, &v)) << "unexpected hit at key=" << k;
    };

    // 命中：头/中/尾
    hit(1);
    hit(N / 2);
    hit(N);

    // 未命中：越界两侧
    miss(0);
    miss(N + 12345);

    // 随机抽样命中（简单采样）
    for (Key k : {23, 456, 789, 1024, 19999})
        hit(k);
}
