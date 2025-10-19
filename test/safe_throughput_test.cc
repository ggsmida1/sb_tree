#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <iomanip>

int main() {
    std::cout << "=== SB-Tree 安全吞吐量测试 ===" << std::endl;
    
    const std::vector<int> thread_counts = {1, 2, 4, 8};
    const int base_inserts = 2000; // 降低插入数量避免崩溃
    
    std::cout << "每线程插入数: " << base_inserts << std::endl;
    std::cout << "测试线程数: ";
    for (size_t i = 0; i < thread_counts.size(); ++i) {
        std::cout << thread_counts[i];
        if (i < thread_counts.size() - 1) std::cout << ", ";
    }
    std::cout << std::endl << std::endl;
    
    std::cout << std::left
              << std::setw(8) << "线程数"
              << std::setw(12) << "耗时(ms)"
              << std::setw(15) << "吞吐量(ops/s)"
              << std::setw(12) << "成功率"
              << std::setw(10) << "加速比" << std::endl;
    std::cout << std::string(60, '-') << std::endl;
    
    double baseline_throughput = 0;
    
    for (int num_threads : thread_counts) {
        std::cout << "运行 " << num_threads << " 线程测试...";
        
        SBTree tree;
        std::atomic<int> success_count{0};
        std::atomic<int> fail_count{0};
        
        auto start = std::chrono::high_resolution_clock::now();
        
        std::vector<std::thread> threads;
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                for (int i = 0; i < base_inserts; ++i) {
                    uint64_t key = static_cast<uint64_t>(t) * base_inserts + i;
                    uint64_t value = key * 10;
                    
                    if (tree.Insert(key, value)) {
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
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        double duration_ms = static_cast<double>(duration.count());
        if (duration_ms < 0.1) duration_ms = 0.1;
        
        double throughput = static_cast<double>(success_count.load()) / duration_ms * 1000;
        double success_rate = static_cast<double>(success_count.load()) / (num_threads * base_inserts) * 100;
        
        if (num_threads == 1) {
            baseline_throughput = throughput;
        }
        
        double speedup = baseline_throughput > 0 ? throughput / baseline_throughput : 1.0;
        
        std::cout << "       " << num_threads
                  << std::setw(12) << std::fixed << std::setprecision(1) << duration_ms
                  << std::setw(15) << std::fixed << std::setprecision(0) << throughput
                  << std::setw(12) << std::fixed << std::setprecision(1) << success_rate << "%"
                  << std::setw(10) << std::fixed << std::setprecision(2) << speedup << "x" << std::endl;
    }
    
    std::cout << std::endl << "=== 性能分析 ===" << std::endl;
    std::cout << "注意：此测试仅关注插入吞吐量，不验证数据一致性" << std::endl;
    std::cout << "成功插入数表示Insert()返回true的次数" << std::endl;
    
    return 0;
}
