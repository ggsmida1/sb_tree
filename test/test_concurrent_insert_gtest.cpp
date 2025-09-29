#include <gtest/gtest.h>
#include <thread>
#include "SBTree.h"

// 并发插入基础测试
TEST(ConcurrentInsert, AllKeysPresentNoDuplicates)
{
    const size_t num_threads = 4;
    const size_t keys_per_thread = 1000;

    SBTree t;

    // 多线程插入
    std::vector<std::thread> threads;
    for (size_t tid = 0; tid < num_threads; ++tid)
    {
        threads.emplace_back([&, tid]()
                             {
            size_t base = tid * keys_per_thread;
            for (size_t k = 0; k < keys_per_thread; ++k) {
                t.insert(base + k, (base + k) * 10);
            } });
    }
    for (auto &th : threads)
        th.join();

    // 插入完成后，做 flush
    t.flush();
    t.flush_index();

    // 扫描全量
    std::vector<uint64_t> out;
    size_t got = t.scan(0, num_threads * keys_per_thread - 1, out);

    // 检查数量
    ASSERT_EQ(got, num_threads * keys_per_thread);

    // 检查内容（单调递增）
    for (size_t i = 0; i < got; ++i)
    {
        ASSERT_EQ(out[i], static_cast<uint64_t>(i * 10));
    }
}
