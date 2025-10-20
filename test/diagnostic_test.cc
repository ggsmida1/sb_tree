#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <cassert>
#include "../include/sb_tree.h"

int main() {
    std::cout << "=== SB-Tree 诊断测试 ===" << std::endl;
    
    SBTree tree;
    const int num_threads = 4;
    const int inserts_per_thread = 1000;
    const int total_inserts = num_threads * inserts_per_thread;
    
    std::cout << "测试配置:" << std::endl;
    std::cout << "  线程数: " << num_threads << std::endl;
    std::cout << "  每线程插入数: " << inserts_per_thread << std::endl;
    std::cout << "  总插入数: " << total_inserts << std::endl;
    std::cout << std::endl;
    
    // 1. 单线程测试
    std::cout << "1. 单线程基准测试..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < total_inserts; ++i) {
        bool success = tree.Insert(i, i * 10);
        if (!success) {
            std::cout << "单线程插入失败: key=" << i << std::endl;
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "单线程耗时: " << duration.count() << " ms" << std::endl;
    
    // 验证单线程结果
    int success_count = 0;
    for (int i = 0; i < total_inserts; ++i) {
        const uint64_t* value = tree.Lookup(i);
        if (value && *value == i * 10) {
            success_count++;
        }
    }
    std::cout << "单线程验证: " << success_count << "/" << total_inserts << " 成功" << std::endl;
    std::cout << std::endl;
    
    // 2. 多线程测试
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
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "多线程耗时: " << duration.count() << " ms" << std::endl;
    std::cout << "插入成功: " << insert_count.load() << std::endl;
    std::cout << "插入失败: " << error_count.load() << std::endl;
    
    // 3. 等待转换器
    std::cout << "3. 等待转换器完成..." << std::endl;
    tree2.WaitForConverterIdle();
    std::cout << "转换器空闲" << std::endl;
    
    // 4. 验证多线程结果
    std::cout << "4. 多线程结果验证..." << std::endl;
    success_count = 0;
    int error_count_verify = 0;
    for (int i = 0; i < total_inserts; ++i) {
        const uint64_t* value = tree2.Lookup(i);
        if (value && *value == i * 10) {
            success_count++;
        } else {
            error_count_verify++;
            if (error_count_verify <= 10) {
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
    
    if (error_count_verify > 10) {
        std::cout << "还有 " << (error_count_verify - 10) << " 个错误..." << std::endl;
    }
    
    std::cout << "多线程验证: " << success_count << "/" << total_inserts << " 成功" << std::endl;
    std::cout << std::endl;
    
    // 5. 性能分析
    std::cout << "=== 性能分析 ===" << std::endl;
    double single_throughput = (double)total_inserts / (duration.count() > 0 ? duration.count() : 1);
    double multi_throughput = (double)insert_count.load() / (duration.count() > 0 ? duration.count() : 1);
    double speedup = single_throughput > 0 ? multi_throughput / single_throughput : 1.0;
    double consistency = (double)success_count / total_inserts * 100.0;
    
    std::cout << "单线程吞吐量: " << single_throughput << " ops/sec" << std::endl;
    std::cout << "多线程吞吐量: " << multi_throughput << " ops/sec" << std::endl;
    std::cout << "吞吐量提升: " << speedup << "x" << std::endl;
    std::cout << "数据一致性: " << consistency << "%" << std::endl;
    
    if (success_count == total_inserts) {
        std::cout << "✅ 测试通过，数据完整" << std::endl;
        return 0;
    } else {
        std::cout << "❌ 测试失败，数据丢失严重" << std::endl;
        return 1;
    }
}
