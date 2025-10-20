#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>

int main() {
    std::cout << "=== 简单多线程插入测试 ===" << std::endl;
    
    const int num_threads = 4;
    const int inserts_per_thread = 2000;
    
    std::cout << "线程数: " << num_threads << std::endl;
    std::cout << "每线程插入数: " << inserts_per_thread << std::endl;
    std::cout << "总插入数: " << (num_threads * inserts_per_thread) << std::endl;
    std::cout << std::endl;
    
    // 单线程测试
    std::cout << "1. 单线程测试..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    
    SBTree single_tree;
    for (int i = 0; i < num_threads * inserts_per_thread; ++i) {
        single_tree.Insert(i, i * 10);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto single_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "单线程耗时: " << single_duration.count() << " ms" << std::endl;
    
    // 验证单线程结果
    int verified = 0;
    for (int i = 0; i < num_threads * inserts_per_thread; ++i) {
        const uint64_t* value = single_tree.Lookup(i);
        if (value && *value == static_cast<uint64_t>(i * 10)) {
            verified++;
        }
    }
    std::cout << "单线程验证: " << verified << "/" << (num_threads * inserts_per_thread) << " 成功" << std::endl;
    std::cout << std::endl;
    
    // 多线程测试
    std::cout << "2. 多线程测试..." << std::endl;
    start = std::chrono::high_resolution_clock::now();
    
    SBTree multi_tree;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    std::atomic<int> fail_count{0};
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < inserts_per_thread; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
                uint64_t value = key * 10;
                
                if (multi_tree.Insert(key, value)) {
                    success_count.fetch_add(1);
                } else {
                    fail_count.fetch_add(1);
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    end = std::chrono::high_resolution_clock::now();
    auto multi_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "多线程耗时: " << multi_duration.count() << " ms" << std::endl;
    std::cout << "成功插入: " << success_count.load() << std::endl;
    std::cout << "失败插入: " << fail_count.load() << std::endl;
    
    // 等待转换器完成所有任务
    std::cout << "3. 等待转换器完成..." << std::endl;
    multi_tree.WaitForConverterIdle();
    
    // 验证多线程结果
    std::cout << "4. 多线程结果验证..." << std::endl;
    int multi_verified = 0;
    int errors = 0;
    
    for (int t = 0; t < num_threads; ++t) {
        for (int i = 0; i < inserts_per_thread; ++i) {
            uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
            uint64_t expected_value = key * 10;
            
            const uint64_t* value = multi_tree.Lookup(key);
            if (value && *value == expected_value) {
                multi_verified++;
            } else {
                errors++;
                if (errors <= 3) {
                    std::cout << "错误: key=" << key << ", expected=" << expected_value;
                    if (value) {
                        std::cout << ", got=" << *value;
                    } else {
                        std::cout << ", got=nullptr";
                    }
                    std::cout << std::endl;
                }
            }
        }
    }
    
    std::cout << "多线程验证: " << multi_verified << "/" << (num_threads * inserts_per_thread) << " 成功" << std::endl;
    if (errors > 3) {
        std::cout << "还有 " << (errors - 3) << " 个错误..." << std::endl;
    }
    
    // 性能分析
    std::cout << std::endl << "=== 性能分析 ===" << std::endl;
    double speedup = static_cast<double>(single_duration.count()) / multi_duration.count();
    double single_throughput = static_cast<double>(num_threads * inserts_per_thread) / single_duration.count() * 1000;
    double multi_throughput = static_cast<double>(success_count.load()) / multi_duration.count() * 1000;
    
    std::cout << "加速比: " << speedup << "x" << std::endl;
    std::cout << "单线程吞吐量: " << single_throughput << " ops/sec" << std::endl;
    std::cout << "多线程吞吐量: " << multi_throughput << " ops/sec" << std::endl;
    std::cout << "吞吐量提升: " << (multi_throughput / single_throughput) << "x" << std::endl;
    
    return 0;
}
