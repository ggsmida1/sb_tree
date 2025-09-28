#include <gtest/gtest.h>
#include "SBTree.h"

// 自探测 PTB 容量并验证“填满即切段”的语义
TEST(AutoDetect, SealOnFill)
{
    SBTree t;
    std::vector<uint64_t> out;

    // 基线：还没有任何转换
    uint64_t enq0 = t.index_batches_enqueued();

    // 逐个插入，直到发现第一次转换入队
    uint64_t k = 0;
    for (;; ++k)
    {
        t.insert(k, k * 10);
        t.flush_index(); // 确保 enqueued 可见
        if (t.index_batches_enqueued() > enq0)
        {
            break;
        }
    }

    // 此时 key=k 应该是“填满位”，刚好触发转换
    // 验证：scan 到 k 这一条应当存在
    size_t got = t.scan(k, k, out);
    ASSERT_EQ(got, 1u);
    ASSERT_EQ(out[0], k * 10);

    // 再验证：接缝左侧 (k-1) 也在，连续两条都在数据层
    out.clear();
    got = t.scan(k - 1, k, out);
    ASSERT_EQ(got, 2u);
    ASSERT_EQ(out, std::vector<uint64_t>({(k - 1) * 10, k * 10}));
}
