#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include "SBTree.h"

TEST(ConcurrentInsert, InterleavedKeysAllThreadsSameRange)
{
    const size_t num_threads = 4; // 并发线程数
    const size_t max_key = 1000;  // 插入区间 [1..1000]
    SBTree t;

    // 多线程插入
    std::vector<std::thread> threads;
    for (size_t tid = 0; tid < num_threads; ++tid)
    {
        threads.emplace_back([&, tid]()
                             {
            for (size_t k = tid + 1; k <= max_key; k += num_threads) {
                t.insert(k, k * 10);
            } });
    }
    for (auto &th : threads)
        th.join();

    // 插入完成后 flush
    t.flush();
    t.flush_index();

    // 扫描全量 [1..1000]
    std::vector<uint64_t> out;
    size_t got = t.scan(1, max_key, out);

    // 断言 1000 条数据完整
    ASSERT_EQ(got, max_key);

    // 断言严格单调递增 & 正确的 (k*10) 映射
    for (size_t i = 0; i < max_key; ++i)
    {
        ASSERT_EQ(out[i], static_cast<uint64_t>((i + 1) * 10));
    }
}
