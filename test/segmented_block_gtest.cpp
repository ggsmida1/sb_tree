#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <vector>
#include "SegmentedBlock.h"
#include "PerThreadDataBlock.h"

namespace
{
size_t compute_ptb_capacity()
{
    PerThreadDataBlock ptb;
    size_t count = 0;
    while (ptb.Insert(count, count))
    {
        ++count;
    }
    return count;
}
} // namespace

TEST(SegmentedBlockSeal, TriggersOnlyWhenAllSlotsFull)
{
    const size_t capacity = compute_ptb_capacity();
    ASSERT_GT(capacity, 1u) << "PTB capacity is unexpectedly small";

    SegmentedBlock seg;
    const int thread_count = 3;

    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<int> finished{0};
    std::atomic<bool> thread_failure{false};
    std::atomic<bool> third_ready{false};
    std::atomic<bool> allow_third_finish{false};

    std::vector<std::thread> threads;
    threads.reserve(thread_count);

    for (int tid = 0; tid < thread_count; ++tid)
    {
        threads.emplace_back([&, tid]()
                             {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire))
            {
                std::this_thread::yield();
            }

            size_t limit = capacity;
            if (tid == thread_count - 1)
            {
                limit = capacity - 1; // hold back the last insert
            }

            for (size_t i = 0; i < limit; ++i)
            {
                if (!seg.append_ordered(static_cast<Key>(tid * capacity + i),
                                        static_cast<Value>(tid * capacity + i)))
                {
                    thread_failure.store(true, std::memory_order_release);
                    return;
                }
            }

            if (tid == thread_count - 1)
            {
                third_ready.store(true, std::memory_order_release);
                while (!allow_third_finish.load(std::memory_order_acquire))
                {
                    std::this_thread::yield();
                }
                if (!seg.append_ordered(static_cast<Key>(tid * capacity + limit),
                                        static_cast<Value>(tid * capacity + limit)))
                {
                    thread_failure.store(true, std::memory_order_release);
                    return;
                }
            }

            finished.fetch_add(1, std::memory_order_release);
        });
    }

    while (ready.load(std::memory_order_acquire) < thread_count)
    {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);

    // Wait until every thread except the throttled one reports completion.
    while (finished.load(std::memory_order_acquire) < thread_count - 1)
    {
        std::this_thread::yield();
    }

    // Ensure the third thread has actually reserved its slot and is paused.
    while (!third_ready.load(std::memory_order_acquire))
    {
        std::this_thread::yield();
    }

    EXPECT_FALSE(seg.should_seal()) << "Segment should not request sealing until all slots are full";

    allow_third_finish.store(true, std::memory_order_release);

    for (auto &th : threads)
    {
        th.join();
    }

    ASSERT_FALSE(thread_failure.load()) << "append_ordered failed unexpectedly";
    EXPECT_TRUE(seg.should_seal()) << "Segment should request sealing after every slot is full";

    auto data = seg.collect_and_sort_data();
    EXPECT_EQ(data.size(), capacity * static_cast<size_t>(thread_count));
    for (size_t i = 1; i < data.size(); ++i)
    {
        EXPECT_LE(data[i - 1].key, data[i].key);
    }
}
