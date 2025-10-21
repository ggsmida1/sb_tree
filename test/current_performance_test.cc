#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <cassert>
#include "../include/sb_tree.h"

void TestPerformance(int num_threads, int inserts_per_thread, const std::string& test_name) {
    std::cout << "\n=== " << test_name << " ===" << std::endl;
    std::cout << "线程数: " << num_threads << ", 每线程插入: " << inserts_per_thread << std::endl;
    
    SBTree tree;
    const int total_inserts = num_threads * inserts_per_thread;
    
    // 单线程基准测试
    std::cout << "1. 单线程基准测试..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < total_inserts; ++i) {
        tree.Insert(i, i * 10);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto single_duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "单线程耗时: " << single_duration.count() << " μs" << std::endl;
    
    // 验证单线程结果
    int success_count = 0;
    for (int i = 0; i < total_inserts; ++i) {
        const uint64_t* value = tree.Lookup(i);
        if (value && *value == i * 10) {
            success_count++;
        }
    }
    std::cout << "单线程验证: " << success_count << "/" << total_inserts << " 成功" << std::endl;
    
    // 多线程测试
    std::cout << "2. 多线程并发测试..." << std::endl;
    SBTree tree2;
    std::vector<std::thread> threads;
    std::atomic<int> insert_count{0};
    std::atomic<int> error_count{0};
    
    start = std::chrono::high_resolution_clock::now();
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&tree2, &insert_count, &error_count, t, inserts_per_thread]() {
            for (int i = 0; i < inserts_per_thread; ++i) {
                int key = t * inserts_per_thread + i;
                bool success = tree2.Insert(key, key * 10);
                if (success) {
                    insert_count.fetch_add(1);
                } else {
                    error_count.fetch_add(1);
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    end = std::chrono::high_resolution_clock::now();
    auto multi_duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "多线程耗时: " << multi_duration.count() << " μs" << std::endl;
    std::cout << "插入成功: " << insert_count.load() << ", 插入失败: " << error_count.load() << std::endl;
    
    // 等待转换器
    std::cout << "3. 等待转换器完成..." << std::endl;
    tree2.WaitForConverterIdle();
    
    // 验证多线程结果
    std::cout << "4. 多线程结果验证..." << std::endl;
    success_count = 0;
    int error_count_verify = 0;
    for (int i = 0; i < total_inserts; ++i) {
        const uint64_t* value = tree2.Lookup(i);
        if (value && *value == i * 10) {
            success_count++;
        } else {
            error_count_verify++;
            if (error_count_verify <= 5) {
                std::cout << "错误: key=" << i << ", expected=" << (i * 10) << ", got=";
                if (value) {
                    std::cout << *value;
                } else {
                    std::cout << "nullptr";
                }
                std::cout << std::endl;
            }
        }
    }
    
    if (error_count_verify > 5) {
        std::cout << "还有 " << (error_count_verify - 5) << " 个错误..." << std::endl;
    }
    
    std::cout << "多线程验证: " << success_count << "/" << total_inserts << " 成功" << std::endl;
    
    // 性能分析
    std::cout << "\n=== 性能分析 ===" << std::endl;
    double single_throughput = (double)total_inserts / (single_duration.count() > 0 ? single_duration.count() : 1) * 1000000;
    double multi_throughput = (double)insert_count.load() / (multi_duration.count() > 0 ? multi_duration.count() : 1) * 1000000;
    double speedup = single_throughput > 0 ? multi_throughput / single_throughput : 1.0;
    double consistency = (double)success_count / total_inserts * 100.0;
    
    std::cout << "单线程吞吐量: " << (int)single_throughput << " ops/sec" << std::endl;
    std::cout << "多线程吞吐量: " << (int)multi_throughput << " ops/sec" << std::endl;
    std::cout << "吞吐量提升: " << speedup << "x" << std::endl;
    std::cout << "数据一致性: " << consistency << "%" << std::endl;
    
    if (success_count == total_inserts) {
        std::cout << "✅ 测试通过" << std::endl;
    } else {
        std::cout << "❌ 测试失败，数据丢失" << std::endl;
    }
}

int main() {
    std::cout << "=== SB-Tree 当前性能测试 ===" << std::endl;
    
    // 测试不同规模的数据
    TestPerformance(1, 100, "小数据量单线程测试");
    TestPerformance(2, 100, "小数据量双线程测试");
    TestPerformance(4, 100, "小数据量四线程测试");
    TestPerformance(4, 500, "中等数据量四线程测试");
    TestPerformance(4, 1000, "大数据量四线程测试");
    
    return 0;
}


