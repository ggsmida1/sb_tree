// main_mt.cpp
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>
#include "SBTree.h"

int main()
{
    SBTree t;

    const int T = 8;     // 线程数
    const int M = 50000; // 每线程插入量
    std::vector<std::thread> ths;

    for (int th = 0; th < T; ++th)
    {
        ths.emplace_back([&, th]
                         {
            uint64_t base = uint64_t(th) << 48; // 让每个线程的 key 各自单调递增
            for (int i = 1; i <= M; ++i) {
                t.insert(base + i, base + i * 10);
            } });
    }
    for (auto &th : ths)
        th.join();

    std::cout << "[OK] multi-thread inserted " << (size_t)T * M << " items\n";

    // ==== DataBlock 并发读测试（只读，构建后不再写） ====
    {
        // 构造一块 DataBlock（已排序 KV）
        std::vector<KVPair> kvs;
        const int N = 5000;
        kvs.reserve(N);
        for (int i = 1; i <= N; ++i)
            kvs.push_back({(Key)i, (Value)(i * 10)});

        DataBlock db;
        size_t take = db.build_from_sorted(kvs.data(), kvs.size()); // 实际写入条数（≤ 容量）

        // 多线程并发执行 find/scan 校验
        const int T = 8;        // 线程数
        const int LOOPS = 2000; // 每线程循环次数
        std::vector<std::thread> ths;
        std::atomic<int> ok_find{0}, ok_scan{0};

        for (int th = 0; th < T; ++th)
        {
            ths.emplace_back([&, th]
                             {
            // 每个线程从不同起点做确定性访问，避免随机数带来的不稳定
            size_t base = (th * 37) % take;

            // 1) 批量 find 校验：key = [base+1, base+LOOPS]（取模到 [1, take]）
            for (int i = 0; i < LOOPS; ++i) {
                Key k = (Key)((base + i) % take + 1);
                Value v = 0;
                bool ok = db.find(k, v);
                if (!(ok && v == (Value)(k * 10))) {
                    // 失败时断言（调试期可触发）；也可以换成返回
                    assert(false && "concurrent find failed");
                }
                ok_find.fetch_add(1, std::memory_order_relaxed);
            }

            // 2) 批量 scan 校验：从某个起点，取固定条数，检查单调递增和值匹配
            const size_t kTake = 8;
            for (int i = 0; i < LOOPS; ++i) {
                Key start = (Key)((base + i) % take + 1);
                std::vector<Value> out;
                size_t got = db.scan_from(start, kTake, out);
                // 因为块内只存 take 条，可能靠近末尾时不足 kTake
                for (size_t j = 0; j < got; ++j) {
                    Key expect_k = start + j;
                    Value expect_v = (Value)(expect_k * 10);
                    assert(out[j] == expect_v);
                }
                ok_scan.fetch_add(1, std::memory_order_relaxed);
            } });
        }
        for (auto &th : ths)
            th.join();

        std::cout << "[OK] DataBlock concurrent read: "
                  << ok_find.load() << " finds, "
                  << ok_scan.load() << " scans\n";
    }

    return 0;
}
