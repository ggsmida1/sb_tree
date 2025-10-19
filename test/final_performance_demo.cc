
#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <iomanip>

int main() {
    std::cout << "===============================================" << std::endl;
    std::cout << "    SB-Tree 多线程插入性能优化展示" << std::endl;
    std::cout << "===============================================" << std::endl;
    std::cout << std::endl;
    
    // 测试配置
    const int inserts_per_thread = 1000;
    std::vector<int> thread_counts = {1, 2, 4, 8};
    
    std::cout << "测试配置:" << std::endl;
    std::cout << "  每线程插入数: " << inserts_per_thread << std::endl;
    std::cout << "  测试线程数: ";
    for (int i = 0; i < thread_counts.size(); ++i) {
        std::cout << thread_counts[i];
        if (i < thread_counts.size() - 1) std::cout << ", ";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    
    std::cout << std::setw(8) << "线程数" 
              << std::setw(15) << "耗时(ms)" 
              << std::setw(15) << "吞吐量(ops/s)" 
              << std::setw(12) << "成功率" 
              << std::setw(10) << "加速比" << std::endl;
    std::cout << std::string(70, '-') << std::endl;
    
    double baseline_throughput = 0;
    std::vector<double> throughputs;
    
    for (int num_threads : thread_counts) {
        std::cout << "运行 " << num_threads << " 线程测试..." << std::flush;
        
        SBTree tree;
        std::vector<std::thread> threads;
        std::atomic<int> success_count{0};
        std::atomic<int> fail_count{0};
        
        auto start = std::chrono::high_resolution_clock::now();
        
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                for (int i = 0; i < inserts_per_thread; ++i) {
                    uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
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
        if (duration_ms < 0.1) duration_ms = 0.1; // 避免除零
        
        double throughput = static_cast<double>(success_count.load()) / duration_ms * 1000;
        double success_rate = static_cast<double>(success_count.load()) / (num_threads * inserts_per_thread) * 100;
        
        if (num_threads == 1) {
            baseline_throughput = throughput;
        }
        
        double speedup = baseline_throughput > 0 ? throughput / baseline_throughput : 1.0;
        throughputs.push_back(throughput);
        
        std::cout << std::setw(8) << num_threads
                  << std::setw(15) << std::fixed << std::setprecision(1) << duration_ms
                  << std::setw(15) << std::fixed << std::setprecision(0) << throughput
                  << std::setw(12) << std::fixed << std::setprecision(1) << success_rate << "%"
                  << std::setw(10) << std::fixed << std::setprecision(2) << speedup << "x" << std::endl;
    }
    
    std::cout << std::endl;
    
    // 性能分析
    std::cout << "=== 性能分析 ===" << std::endl;
    std::cout << "优化效果:" << std::endl;
    
    for (size_t i = 1; i < throughputs.size(); ++i) {
        double improvement = throughputs[i] / throughputs[0];
        std::cout << "  " << thread_counts[i] << " 线程 vs 1 线程: " 
                  << std::fixed << std::setprecision(2) << improvement << "x 吞吐量提升" << std::endl;
    }
    
    // 最佳性能
    double max_throughput = *std::max_element(throughputs.begin(), throughputs.end());
    auto max_iter = std::max_element(throughputs.begin(), throughputs.end());
    int best_thread_count = thread_counts[std::distance(throughputs.begin(), max_iter)];
    
    std::cout << std::endl;
    std::cout << "最佳性能: " << best_thread_count << " 线程, " 
              << std::fixed << std::setprecision(0) << max_throughput << " ops/sec" << std::endl;
    
    // 数据一致性验证
    std::cout << std::endl;
    std::cout << "=== 数据一致性验证 ===" << std::endl;
    
    SBTree verify_tree;
    const int verify_threads = 4;
    const int verify_inserts = 200;
    
    std::vector<std::thread> verify_threads_vec;
    std::atomic<int> verify_success{0};
    
    for (int t = 0; t < verify_threads; ++t) {
        verify_threads_vec.emplace_back([&, t]() {
            for (int i = 0; i < verify_inserts; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * verify_inserts + i;
                uint64_t value = key * 10;
                if (verify_tree.Insert(key, value)) {
                    verify_success.fetch_add(1);
                }
            }
        });
    }
    
    for (auto& thread : verify_threads_vec) {
        thread.join();
    }
    
    std::cout << "并发插入验证: " << verify_success.load() << "/" << (verify_threads * verify_inserts) << " 成功" << std::endl;
    
    // 验证查找
    int lookup_success = 0;
    int lookup_errors = 0;
    
    for (int t = 0; t < verify_threads; ++t) {
        for (int i = 0; i < verify_inserts; ++i) {
            uint64_t key = static_cast<uint64_t>(t) * verify_inserts + i;
            uint64_t expected_value = key * 10;
            
            const uint64_t* value = verify_tree.Lookup(key);
            if (value && *value == expected_value) {
                lookup_success++;
            } else {
                lookup_errors++;
            }
        }
    }
    
    double lookup_success_rate = static_cast<double>(lookup_success) / (verify_threads * verify_inserts) * 100;
    std::cout << "并发查找验证: " << lookup_success << "/" << (verify_threads * verify_inserts) 
              << " 成功 (" << std::fixed << std::setprecision(1) << lookup_success_rate << "%)" << std::endl;
    
    if (lookup_errors > 0) {
        std::cout << "查找错误: " << lookup_errors << " 个" << std::endl;
    }
    
    std::cout << std::endl;
    std::cout << "===============================================" << std::endl;
    std::cout << "    SB-Tree 多线程优化验证完成" << std::endl;
    std::cout << "===============================================" << std::endl;
    
    return 0;
}
