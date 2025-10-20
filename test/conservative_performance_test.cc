#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <iomanip>

int main() {
    std::cout << "=== SB-Tree 保守性能测试 ===" << std::endl;
    std::cout << std::endl;
    
    // 测试配置
    const int inserts_per_thread = 500;
    std::vector<int> thread_counts = {1, 2, 4};
    
    std::cout << "每线程插入数: " << inserts_per_thread << std::endl;
    std::cout << std::endl;
    
    std::cout << std::setw(8) << "线程数" 
              << std::setw(12) << "耗时(ms)" 
              << std::setw(12) << "吞吐量" 
              << std::setw(12) << "成功率" << std::endl;
    std::cout << std::string(50, '-') << std::endl;
    
    double baseline_throughput = 0;
    
    for (int num_threads : thread_counts) {
        std::cout << "测试 " << num_threads << " 线程..." << std::flush;
        
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
        double throughput = static_cast<double>(success_count.load()) / duration_ms * 1000;
        double success_rate = static_cast<double>(success_count.load()) / (num_threads * inserts_per_thread) * 100;
        
        if (num_threads == 1) {
            baseline_throughput = throughput;
        }
        
        // double speedup = baseline_throughput > 0 ? throughput / baseline_throughput : 1.0;  // 暂时注释掉未使用的变量
        
        std::cout << std::setw(8) << num_threads
                  << std::setw(12) << std::fixed << std::setprecision(1) << duration_ms
                  << std::setw(12) << std::fixed << std::setprecision(0) << throughput
                  << std::setw(12) << std::fixed << std::setprecision(1) << success_rate << "%" << std::endl;
    }
    
    std::cout << std::endl;
    
    // 验证测试
    std::cout << "=== 数据一致性验证 ===" << std::endl;
    SBTree verify_tree;
    const int verify_threads = 2;
    const int verify_inserts = 100;
    
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
    
    std::cout << "验证插入: " << verify_success.load() << "/" << (verify_threads * verify_inserts) << std::endl;
    
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
                if (lookup_errors <= 3) {
                    std::cout << "查找错误: key=" << key << ", expected=" << expected_value;
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
    
    std::cout << "验证查找: " << lookup_success << "/" << (verify_threads * verify_inserts) << " 成功" << std::endl;
    if (lookup_errors > 3) {
        std::cout << "还有 " << (lookup_errors - 3) << " 个查找错误" << std::endl;
    }
    
    std::cout << std::endl << "=== 测试完成 ===" << std::endl;
    return 0;
}
