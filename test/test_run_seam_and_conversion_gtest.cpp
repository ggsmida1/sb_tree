// tests/test_run_seam_and_conversion_gtest.cpp
#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <cstdint>
#include "SBTree.h"

using namespace std::chrono_literals;

static std::vector<uint64_t> seq_values(uint64_t start, size_t len)
{
    std::vector<uint64_t> v;
    v.reserve(len);
    for (size_t i = 0; i < len; ++i)
        v.push_back((start + i) * 10);
    return v;
}

// 按你工程当前配置：PerThreadDataBlock 实际容量（如调整请同步这里）
static constexpr size_t kPTB_CAP = 1023;

/************* 用例一：Run 接缝不重复（填满即切段语义） *************/
/*
 * 新语义下：
 * - 插入 0..1021 后，段仍 ACTIVE；
 * - 插入 1022（正好填满）→ 立刻切段+转换旧段 → 0..1022 进入数据层；
 * - 再插 1023..1028 落到新段；flush() 后也进入数据层。
 * 断言：scan [1018..1028] 共 11 条，严格等于理想序列。
 */
TEST(RunSeam, NoDuplicateAtBoundary_SealOnFill)
{
    SBTree t;

    // 0..1021：尚未切段
    for (uint64_t k = 0; k < kPTB_CAP - 1; ++k)
        t.insert(k, k * 10);

    // 1022：填满 → 立刻切段并转换旧段（0..1022 进入数据层）
    t.insert(kPTB_CAP - 1, (kPTB_CAP - 1) * 10);

    // 接缝右端补齐 1023..1028
    for (uint64_t k = kPTB_CAP; k <= kPTB_CAP + 5; ++k)
        t.insert(k, k * 10);

    // 将活跃段（含 1023..1028）也转换；索引层刷到位更稳妥
    t.flush();
    t.flush_index();

    const uint64_t L = kPTB_CAP - 5; // 1018
    const uint64_t R = kPTB_CAP + 5; // 1028
    std::vector<uint64_t> out;
    size_t got = t.scan(L, R, out);

    ASSERT_EQ(got, static_cast<size_t>((R - L) + 1))
        << "Scan count mismatch around seam";
    ASSERT_EQ(out, seq_values(L, (R - L) + 1))
        << "Seam data duplicated or missing";
}

/************* 用例二：只有一个转换者（填满即切段语义） *************/
/*
 * 思路：
 * - 预热到 0..1021；
 * - 记录 enq/app 基线；
 * - 主线程先写 1022（立刻切段+转换 → 只会入队一次 run）；
 * - 再放开工人线程写 1023.. 作为并发压力（落到新段，不会再触发转换）；
 * - flush_index() 后，断言 enq/app 差量 == 1；
 * - 最后 flush() 再 scan 验证接缝附近不丢不重。
 */
TEST(Conversion, OnlyOneConverterOnFill)
{
    SBTree t;

    // 预热：0..1021（差一条满）
    for (uint64_t k = 0; k < kPTB_CAP - 1; ++k)
        t.insert(k, k * 10);

    // 记录并发阶段开始前的索引队列计数
    const uint64_t enq0 = t.index_batches_enqueued();
    const uint64_t app0 = t.index_batches_applied();

    // 并发设置（T 个工人；主线程先触发切段，再放开工人）
    const int T = 8;
    std::atomic<bool> go{false};
    std::vector<std::thread> th;
    th.reserve(T);

    // 工人：插 1023..1030（8 条，覆盖到你扫描的右端 1030）
    for (int i = 0; i < T; ++i)
    {
        th.emplace_back([&, i]
                        {
        while (!go.load(std::memory_order_acquire)) { /*spin*/ }
        uint64_t key = (kPTB_CAP) + i; // 1023..1030
        t.insert(key, key*10); });
    }

    // 关键：主线程先写“填满位”1022 → 立刻切段+转换
    t.insert(kPTB_CAP - 1, (kPTB_CAP - 1) * 10);

    // 让工人并发插入（落到新段）
    go.store(true, std::memory_order_release);
    for (auto &x : th)
        x.join();

    // 等索引应用完成（只检查并发阶段的差量）
    t.flush_index();
    const uint64_t enq1 = t.index_batches_enqueued();
    const uint64_t app1 = t.index_batches_applied();
    ASSERT_EQ(enq1 - enq0, 1u);
    ASSERT_EQ(app1 - app0, 1u);

    // 把新段也转换，再做区间校验
    t.flush();
    t.flush_index();

    const uint64_t L = kPTB_CAP - 5;       // 1018
    const uint64_t R = kPTB_CAP + (T - 1); // 1030（现在确实插入到 1030 了）
    std::vector<uint64_t> out;
    size_t got = t.scan(L, R, out);
    ASSERT_EQ(got, static_cast<size_t>((R - L) + 1));
    ASSERT_EQ(out, seq_values(L, (R - L) + 1));
}
