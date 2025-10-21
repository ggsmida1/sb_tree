#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>

int main() {
    std::cout << "=== SB-Tree 逐步复杂度测试 ===" << std::endl;
    
    // 测试1：单线程小规模
    std::cout << "测试1：单线程小规模 (100个元素)..." << std::endl;
    SBTree tree1;
    for (int i = 0; i < 100; ++i) {
        tree1.Insert(i, i * 10);
    }
    tree1.WaitForConverterIdle();
    
    int found1 = 0;
    for (int i = 0; i < 100; ++i) {
        const uint64_t* value = tree1.Lookup(i);
        if (value && *value == static_cast<uint64_t>(i * 10)) {
            found1++;
        }
    }
    std::cout << "单线程小规模: " << found1 << "/100 成功" << std::endl;
    
    // 测试2：单线程中规模
    std::cout << "测试2：单线程中规模 (1000个元素)..." << std::endl;
    SBTree tree2;
    for (int i = 0; i < 1000; ++i) {
        tree2.Insert(i, i * 10);
    }
    tree2.WaitForConverterIdle();
    
    int found2 = 0;
    for (int i = 0; i < 1000; ++i) {
        const uint64_t* value = tree2.Lookup(i);
        if (value && *value == static_cast<uint64_t>(i * 10)) {
            found2++;
        }
    }
    std::cout << "单线程中规模: " << found2 << "/1000 成功" << std::endl;
    
    // 测试3：2线程小规模
    std::cout << "测试3：2线程小规模 (每线程100个)..." << std::endl;
    SBTree tree3;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    
    for (int t = 0; t < 2; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < 100; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * 100 + i;
                if (tree3.Insert(key, key * 10)) {
                    success_count.fetch_add(1);
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    tree3.WaitForConverterIdle();
    
    int found3 = 0;
    for (int t = 0; t < 2; ++t) {
        for (int i = 0; i < 100; ++i) {
            uint64_t key = static_cast<uint64_t>(t) * 100 + i;
            const uint64_t* value = tree3.Lookup(key);
            if (value && *value == key * 10) {
                found3++;
            }
        }
    }
    std::cout << "2线程小规模: 插入" << success_count.load() << "个, 验证" << found3 << "/200 成功" << std::endl;
    
    // 测试4：4线程中规模
    std::cout << "测试4：4线程中规模 (每线程500个)..." << std::endl;
    SBTree tree4;
    threads.clear();
    success_count.store(0);
    
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < 500; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * 500 + i;
                if (tree4.Insert(key, key * 10)) {
                    success_count.fetch_add(1);
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    tree4.WaitForConverterIdle();
    
    int found4 = 0;
    for (int t = 0; t < 4; ++t) {
        for (int i = 0; i < 500; ++i) {
            uint64_t key = static_cast<uint64_t>(t) * 500 + i;
            const uint64_t* value = tree4.Lookup(key);
            if (value && *value == key * 10) {
                found4++;
            }
        }
    }
    std::cout << "4线程中规模: 插入" << success_count.load() << "个, 验证" << found4 << "/2000 成功" << std::endl;
    
    // 测试5：8线程大规模
    std::cout << "测试5：8线程大规模 (每线程1000个)..." << std::endl;
    SBTree tree5;
    threads.clear();
    success_count.store(0);
    
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < 1000; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * 1000 + i;
                if (tree5.Insert(key, key * 10)) {
                    success_count.fetch_add(1);
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    tree5.WaitForConverterIdle();
    
    int found5 = 0;
    for (int t = 0; t < 8; ++t) {
        for (int i = 0; i < 1000; ++i) {
            uint64_t key = static_cast<uint64_t>(t) * 1000 + i;
            const uint64_t* value = tree5.Lookup(key);
            if (value && *value == key * 10) {
                found5++;
            }
        }
    }
    std::cout << "8线程大规模: 插入" << success_count.load() << "个, 验证" << found5 << "/8000 成功" << std::endl;
    
    // 测试6：检查分段块数据
    std::cout << "测试6：检查分段块数据..." << std::endl;
    int segmented_found = 0;
    for (int i = 0; i < 100; ++i) {
        uint64_t value;
        if (tree5.LookupInSegmentedBlock(i, &value) && value == static_cast<uint64_t>(i * 10)) {
            segmented_found++;
        }
    }
    std::cout << "分段块中前100个: 找到" << segmented_found << "个" << std::endl;
    
    std::cout << "=== 测试完成 ===" << std::endl;
    return 0;
}
