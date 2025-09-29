#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include "SBTree.h"

// 写线程单调写入；读线程并发 scan/lookup
TEST(RowexConcurrent, InsertSingleWriterReadersConcurrent)
{
    SBTree t;

    const uint64_t N = 20'000; // 总写入量
    std::atomic<bool> stop{false};
    std::atomic<size_t> scans_ok{0};

    // 读线程：不停扫描小区间，验证不崩溃且结果单调
    std::thread reader([&]
                       {
        std::vector<uint64_t> out;
        uint64_t last_first = 0;
        while (!stop.load(std::memory_order_acquire)) {
            out.clear();
            size_t got = t.scan(0, 200, out);
            if (got > 1) {
                for (size_t i = 1; i < out.size(); ++i) {
                    ASSERT_LE(out[i-1], out[i]);
                }
                if (last_first) ASSERT_GE(out.front(), last_first);
                last_first = out.front();
            }
            scans_ok.fetch_add(1, std::memory_order_relaxed);
        } });

    // 单写线程：严格单调写入，触发多次转换
    std::thread writer([&]
                       {
        for (uint64_t k = 0; k < N; ++k) {
            t.insert(k, k * 10);
        }
        t.flush();
        t.flush_index();
        stop.store(true, std::memory_order_release); });

    writer.join();
    reader.join();

    ASSERT_GT(scans_ok.load(), 100u);

    // 写完后的完整性校验
    std::vector<uint64_t> all;
    size_t got = t.scan(0, N - 1, all);
    ASSERT_EQ(got, N);
    for (uint64_t i = 0; i < N; ++i)
    {
        ASSERT_EQ(all[i], i * 10);
    }
}
