// test_concurrent_gtest.cpp
#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <vector>
#include "SBTree.h"

// ================================================================
// 1. 并发插入：同区间交错键写入（符合论文语义）
// ================================================================
// 每个线程有自己的 PTB，但所有线程都写入同一个 key 区间 [1..max_key]。
// 插入顺序交错，最终转换阶段由 SegmentedBlock 收集并排序，确保全局有序。
TEST(ConcurrentInsert, InterleavedAllThreadsSameRange)
{
    const size_t num_threads = 4;
    const size_t max_key = 4000;
    SBTree t;

    std::vector<std::thread> threads;
    for (size_t tid = 0; tid < num_threads; ++tid)
    {
        threads.emplace_back([&, tid]()
                             {
            // 每个线程写入 key ≡ tid (mod num_threads) 的子序列
            for (size_t k = tid + 1; k <= max_key; k += num_threads)
                t.insert(k, k * 10); });
    }
    for (auto &th : threads)
        th.join();

    // 转换所有段 + 构建索引
    t.flush();
    t.flush_index();

    // 验证全区间完整且严格单调
    std::vector<uint64_t> out;
    size_t got = t.scan(1, max_key, out);
    ASSERT_EQ(got, max_key) << "some keys missing after concurrent insert";
    for (size_t i = 0; i < got; ++i)
        EXPECT_EQ(out[i], static_cast<uint64_t>((i + 1) * 10))
            << "mismatch at key=" << (i + 1);
}

// ================================================================
// 3. Rowex 风格并发读写：单写线程 + 多读线程
// ================================================================
// 写线程持续单调写入，读线程不断扫描验证数据一致性与非崩溃性。
TEST(RowexConcurrent, InsertSingleWriterReadersConcurrent)
{
    SBTree t;
    const uint64_t N = 20000; // 总写入量
    std::atomic<bool> stop{false};
    std::atomic<size_t> scans_ok{0};

    // 读线程：持续扫描小区间 [0..200]
    std::thread reader([&]()
                       {
        std::vector<uint64_t> out;
        uint64_t last_first = 0;
        while (!stop.load(std::memory_order_acquire)) {
            out.clear();
            size_t got = t.scan(0, 200, out);
            if (got > 1) {
                for (size_t i = 1; i < out.size(); ++i)
                    ASSERT_LE(out[i-1], out[i]);
                if (last_first) ASSERT_GE(out.front(), last_first);
                last_first = out.front();
            }
            scans_ok.fetch_add(1, std::memory_order_relaxed);
        } });

    // 写线程：单调插入，周期性触发转换
    std::thread writer([&]()
                       {
        for (uint64_t k = 0; k < N; ++k)
            t.insert(k, k * 10);
        t.flush();
        t.flush_index();
        stop.store(true, std::memory_order_release); });

    writer.join();
    reader.join();
    ASSERT_GT(scans_ok.load(), 100u) << "reader loop too short";

    // 验证写完后数据完整
    std::vector<uint64_t> all;
    size_t got = t.scan(0, N - 1, all);
    ASSERT_EQ(got, N);
    for (uint64_t i = 0; i < N; ++i)
        ASSERT_EQ(all[i], i * 10);
}