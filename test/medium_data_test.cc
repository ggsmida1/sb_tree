#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <cassert>

void TestMediumData4Threads() {
    std::cout << "=== 4线程中等数据量测试 ===" << std::endl;
    
    SBTree tree;
    const int num_threads = 4;
    const int inserts_per_thread = 5000;  // 每线程5000条，总共20000条
    const int total_inserts = num_threads * inserts_per_thread;
    
    std::cout << "测试配置:" << std::endl;
    std::cout << "  线程数: " << num_threads << std::endl;
    std::cout << "  每线程插入数: " << inserts_per_thread << std::endl;
    std::cout << "  总插入数: " << total_inserts << std::endl;
    std::cout << std::endl;
    
    // 1. 单线程基准测试
    std::cout << "1. 单线程基准测试..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < total_inserts; ++i) {
        if (!tree.Insert(i, i * 10)) {
            std::cout << "单线程插入失败: " << i << std::endl;
            break;
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto single_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "单线程耗时: " << single_duration.count() << " ms" << std::endl;
    
    // 等待转换器完成
    tree.WaitForConverterIdle();
    
    // 验证单线程结果
    int single_success = 0;
    for (int i = 0; i < total_inserts; ++i) {
        const uint64_t* value = tree.Lookup(i);
        if (value && *value == i * 10) {
            single_success++;
        }
    }
    std::cout << "单线程验证: " << single_success << "/" << total_inserts << " 成功" << std::endl;
    std::cout << std::endl;
    
    // 2. 4线程并发测试
    std::cout << "2. 4线程并发测试..." << std::endl;
    
    // 重新创建树进行多线程测试
    SBTree multi_tree;
    std::vector<std::thread> threads;
    std::vector<int> thread_success(num_threads, 0);
    
    start = std::chrono::high_resolution_clock::now();
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&multi_tree, &thread_success, t, inserts_per_thread]() {
            for (int i = 0; i < inserts_per_thread; ++i) {
                uint64_t key = t * inserts_per_thread + i;
                uint64_t value = key * 10;
                if (multi_tree.Insert(key, value)) {
                    thread_success[t]++;
                }
            }
        });
    }
    
    // 等待所有线程完成
    for (auto& thread : threads) {
        thread.join();
    }
    
    end = std::chrono::high_resolution_clock::now();
    auto multi_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "多线程耗时: " << multi_duration.count() << " ms" << std::endl;
    
    // 等待转换器完成
    std::cout << "3. 等待转换器完成..." << std::endl;
    multi_tree.WaitForConverterIdle();
    
    // 4. 验证结果
    std::cout << "4. 多线程结果验证..." << std::endl;
    int multi_success = 0;
    int errors = 0;
    
    for (int t = 0; t < num_threads; ++t) {
        for (int i = 0; i < inserts_per_thread; ++i) {
            uint64_t key = t * inserts_per_thread + i;
            const uint64_t* value = multi_tree.Lookup(key);
            if (value && *value == key * 10) {
                multi_success++;
            } else {
                errors++;
                if (errors <= 10) {  // 只显示前10个错误
                    std::cout << "错误: key=" << key << ", expected=" << key * 10 
                              << ", got=" << (value ? std::to_string(*value) : "nullptr") << std::endl;
                }
            }
        }
    }
    
    std::cout << "多线程验证: " << multi_success << "/" << total_inserts << " 成功" << std::endl;
    if (errors > 10) {
        std::cout << "还有 " << (errors - 10) << " 个错误..." << std::endl;
    }
    std::cout << std::endl;
    
    // 5. 性能分析
    std::cout << "=== 性能分析 ===" << std::endl;
    double single_throughput = total_inserts * 1000.0 / single_duration.count();
    double multi_throughput = total_inserts * 1000.0 / multi_duration.count();
    double speedup = multi_throughput / single_throughput;
    
    std::cout << "单线程吞吐量: " << single_throughput << " ops/sec" << std::endl;
    std::cout << "多线程吞吐量: " << multi_throughput << " ops/sec" << std::endl;
    std::cout << "吞吐量提升: " << speedup << "x" << std::endl;
    std::cout << "数据一致性: " << (multi_success * 100.0 / total_inserts) << "%" << std::endl;
    
    if (multi_success == total_inserts) {
        std::cout << "✅ 测试完全成功！" << std::endl;
    } else if (multi_success > total_inserts * 0.95) {
        std::cout << "⚠️ 测试基本成功，有少量数据丢失" << std::endl;
    } else {
        std::cout << "❌ 测试失败，数据丢失严重" << std::endl;
    }
}

int main() {
    TestMediumData4Threads();
    return 0;
}
