#include <iostream>
#include <random>
#include "SBTree.h"
#include "KVPair.h"

int main()
{
    SBTree t;

    // 1) 插入顺序键
    const Key N = 10000;
    for (Key i = 1; i <= N; ++i)
    {
        t.insert(i, static_cast<Value>(i * 10));
    }

    // 2) 触发转换 + 搜索层构建
    t.flush();

    // === scan_from 简单验证 ===
    auto scan_check = [&](Key start, size_t count, const std::vector<Value> &expect)
    {
        std::vector<Value> out;
        size_t got = t.scan_from(start, count, out);

        bool ok = (got == expect.size()) && (out == expect);
        std::cout << "[scan_from] start=" << start
                  << " count=" << count
                  << " got=" << got
                  << (ok ? " [OK]\n" : " [MISMATCH]\n");
        if (!ok)
        {
            std::cout << "  out: ";
            for (auto v : out)
                std::cout << v << " ";
            std::cout << "\n  exp: ";
            for (auto v : expect)
                std::cout << v << " ";
            std::cout << "\n";
        }
    };

    // 1) 单块内：从 1 开始取 5 个 -> 10,20,30,40,50
    scan_check(/*start=*/1, /*count=*/5, {10, 20, 30, 40, 50});

    // 2) 跨块：从 48 开始取 10 个（已知 DataBlock 容量≈50，会跨到下一块）
    scan_check(48, 10, {480, 490, 500, 510, 520, 530, 540, 550, 560, 570});

    // 3) 从最小值之前开始：start=0
    scan_check(0, 3, {10, 20, 30});

    // 4) 尾部：start=N-3，请求 10 个，但只会拿到 4 个（N=你前面 main 里插的最大键）
    {
        std::vector<Value> out;
        size_t got = t.scan_from(/*start=*/N - 3, /*count=*/10, out);
        std::cout << "[scan_from] start=" << (N - 3)
                  << " count=10 got=" << got << " (expect 4)\n";

        std::vector<Value> exp;
        for (Key k = N - 3; k <= N; ++k)
            exp.push_back(static_cast<Value>(k * 10));
        if (out != exp)
        {
            std::cout << "  out: ";
            for (auto v : out)
                std::cout << v << " ";
            std::cout << "\n  exp: ";
            for (auto v : exp)
                std::cout << v << " ";
            std::cout << "\n";
        }
    }

    // 3) 点查：命中
    auto hit = [&](Key k)
    {
        Value v{};
        bool ok = t.lookup(k, &v);
        std::cout << "[lookup] key=" << k
                  << " ok=" << ok
                  << (ok ? (", value=" + std::to_string(v)) : "")
                  << "\n";
    };

    // 4) 点查：未命中
    auto miss = [&](Key k)
    {
        Value v{};
        bool ok = t.lookup(k, &v);
        std::cout << "[lookup] key=" << k << " ok=" << ok << " (expected miss)\n";
    };

    // 一些基本查询
    hit(1);
    hit(N / 2);
    hit(N);
    miss(0);
    miss(N + 123);

    // 随机抽样命中
    std::mt19937_64 rng(123);
    std::uniform_int_distribution<Key> dist(1, N);
    for (int i = 0; i < 5; ++i)
        hit(dist(rng));

    {
        std::vector<Value> out;
        size_t got = t.scan_range(/*l=*/9950, /*r=*/10010, out);
        std::cout << "[scan_range] [9950,10010] got=" << got << " -> ";
        for (auto v : out)
            std::cout << v << " ";
        std::cout << "\n";
    }

    return 0;
}
