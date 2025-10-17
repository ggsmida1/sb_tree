#include "SBTree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>

static void expect_true(bool cond, const char *msg, int &fails)
{
    if (!cond)
    {
        std::cout << "[FAIL] " << msg << std::endl;
        ++fails;
    }
}

int main()
{
    std::cout << "[Functional] SBTree correctness smoke test" << std::endl;

    SBTree tree;
    int fails = 0;

    // 1) 顺序插入一段区间
    for (int i = 100; i < 200; ++i)
        tree.insert((Key)i, (Value)(i * 10));

    // 2) 插入延迟数据（历史与中间）
    tree.insert(50, 500);   // 历史
    tree.insert(150, 1500); // 中间

    // 3) 点查验证
    Value out = 0;
    expect_true(tree.find(50, out) && out == 500, "find(50) should be 500", fails);
    expect_true(tree.find(150, out) && out == 1500, "find(150) should be 1500", fails);
    expect_true(tree.find(199, out) && out == 1990, "find(199) should be 1990", fails);

    // 4) 扫描验证（覆盖延迟数据与顺序数据）
    std::vector<Value> vec;
    size_t cnt = tree.scan(45, 155, vec);
    expect_true(cnt >= 2, "scan(45,155) should include delayed and in-range values", fails);

    // 5) 多线程小规模并发插入 + 查找抽样
    {
        const int kThreads = 4;
        const int kN = 5000;
        std::atomic<int> ready{0};
        std::atomic<bool> go{false};
        std::vector<std::thread> ths;
        ths.reserve(kThreads);
        for (int t = 0; t < kThreads; ++t)
        {
            ths.emplace_back([&, t]() {
                ready.fetch_add(1);
                while (!go.load())
                    std::this_thread::yield();
                Key base = 1'000'000 + t * 10'000;
                for (int i = 0; i < kN; ++i)
                    tree.insert(base + i, base + i);
            });
        }
        while (ready.load() < kThreads)
            std::this_thread::yield();
        go.store(true);
        for (auto &th : ths)
            th.join();

        // 抽样验证
        for (int t = 0; t < kThreads; ++t)
        {
            Key k = 1'000'000 + t * 10'000 + 123;
            Value v = 0;
            expect_true(tree.find(k, v) && v == k, "mt find sample", fails);
        }
    }

    if (fails == 0)
        std::cout << "[Functional] PASS" << std::endl;
    else
        std::cout << "[Functional] FAILS = " << fails << std::endl;

    return fails == 0 ? 0 : 1;
}


