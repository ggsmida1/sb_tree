// test/test_rowex_levels_gtest.cpp
#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include "SBTree.h"

TEST(RowexLevels, NonBlockingDuringConcurrentInsert)
{
    SBTree t;

    std::atomic<bool> stop{false};
    std::atomic<size_t> reads{0};
    std::atomic<size_t> last_lv{0};
    std::atomic<bool> ok_monotonic{true};

    // 读线程：不停读取 index_levels()，检查不阻塞且单调不降
    std::thread reader([&]
                       {
        size_t prev = 0;
        while (!stop.load(std::memory_order_acquire)) {
            size_t lv = t.index_levels();
            reads.fetch_add(1, std::memory_order_relaxed);
            if (lv < prev) ok_monotonic.store(false, std::memory_order_relaxed);
            prev = lv;
            last_lv.store(lv, std::memory_order_relaxed);
        } });

    // 写线程：持续插入，制造多个 run / 可能的层级晋升
    std::thread writer([&]
                       {
        // 插入多轮，每轮触发一次转换（PTB 填满即切段）
        const size_t rounds = 5;
        const size_t ptb_cap = 1023; // 你当前 PTB 容量；若以后调整，测试仍然有效
        uint64_t k = 0;
        for (size_t r = 0; r < rounds; ++r) {
            // 填满并触发转换
            for (size_t i = 0; i < ptb_cap - 1; ++i) t.insert(k++, k*10);
            t.insert(k++, k*10); // 触发切段+转换
        }
        t.flush();
        t.flush_index();
        stop.store(true, std::memory_order_release); });

    writer.join();
    reader.join();

    // 至少读了很多次，说明不阻塞
    ASSERT_GT(reads.load(), 100u);
    // 层级应该 >= 1（至少有叶层）
    ASSERT_GE(last_lv.load(), 1u);
    // 层级读数不应倒退（弱条件，允许相等）
    ASSERT_TRUE(ok_monotonic.load());
}
