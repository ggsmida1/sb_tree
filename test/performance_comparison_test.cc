#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <iomanip>

class PerformanceComparison {
public:
    static void RunComparison() {
        std::cout << "=== SB-Tree 多线程性能对比测试 ===" << std::endl;
        std::cout << std::endl;
        
        // 测试不同线程数的性能
        std::vector<int> thread_counts = {1, 2, 4, 8};
        const int inserts_per_thread = 2000;
        
        std::cout << "每线程插入数: " << inserts_per_thread << std::endl;
        std::cout << std::endl;
        
        std::cout << std::setw(8) << "线程数" 
                  << std::setw(12) << "耗时(ms)" 
                  << std::setw(12) << "吞吐量" 
                  << std::setw(12) << "成功率" 
                  << std::setw(12) << "加速比" << std::endl;
        std::cout << std::string(60, '-') << std::endl;
        
        double baseline_throughput = 0;
        
        for (int num_threads : thread_counts) {
            auto result = RunTest(num_threads, inserts_per_thread);
            
            double throughput = static_cast<double>(result.successful_inserts) / result.duration_ms * 1000;
            double success_rate = static_cast<double>(result.successful_inserts) / (num_threads * inserts_per_thread) * 100;
            double speedup = baseline_throughput > 0 ? throughput / baseline_throughput : 1.0;
            
            if (num_threads == 1) {
                baseline_throughput = throughput;
            }
            
            std::cout << std::setw(8) << num_threads
                      << std::setw(12) << std::fixed << std::setprecision(1) << result.duration_ms
                      << std::setw(12) << std::fixed << std::setprecision(0) << throughput
                      << std::setw(12) << std::fixed << std::setprecision(1) << success_rate << "%"
                      << std::setw(12) << std::fixed << std::setprecision(2) << speedup << "x" << std::endl;
        }
        
        std::cout << std::endl;
        
        // 高并发压力测试
        std::cout << "=== 高并发压力测试 ===" << std::endl;
        RunStressTest();
    }
    
private:
    struct TestResult {
        double duration_ms;
        int successful_inserts;
        int failed_inserts;
    };
    
    static TestResult RunTest(int num_threads, int inserts_per_thread) {
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
        
        return {static_cast<double>(duration.count()), success_count.load(), fail_count.load()};
    }
    
    static void RunStressTest() {
        const int stress_threads = 16;
        const int stress_inserts = 1000;
        
        std::cout << "压力测试: " << stress_threads << " 线程, 每线程 " << stress_inserts << " 次插入" << std::endl;
        
        SBTree stress_tree;
        std::vector<std::thread> stress_threads_vec;
        std::atomic<int> stress_success{0};
        std::atomic<int> stress_failures{0};
        
        auto start = std::chrono::high_resolution_clock::now();
        
        for (int t = 0; t < stress_threads; ++t) {
            stress_threads_vec.emplace_back([&, t]() {
                for (int i = 0; i < stress_inserts; ++i) {
                    uint64_t key = static_cast<uint64_t>(t) * stress_inserts + i;
                    uint64_t value = key * 7;
                    
                    if (stress_tree.Insert(key, value)) {
                        stress_success.fetch_add(1);
                    } else {
                        stress_failures.fetch_add(1);
                    }
                }
            });
        }
        
        for (auto& thread : stress_threads_vec) {
            thread.join();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        double throughput = static_cast<double>(stress_success.load()) / duration.count() * 1000;
        double success_rate = static_cast<double>(stress_success.load()) / (stress_threads * stress_inserts) * 100;
        
        std::cout << "压力测试结果:" << std::endl;
        std::cout << "  耗时: " << duration.count() << " ms" << std::endl;
        std::cout << "  成功: " << stress_success.load() << std::endl;
        std::cout << "  失败: " << stress_failures.load() << std::endl;
        std::cout << "  吞吐量: " << std::fixed << std::setprecision(0) << throughput << " ops/sec" << std::endl;
        std::cout << "  成功率: " << std::fixed << std::setprecision(1) << success_rate << "%" << std::endl;
        
        // 验证部分结果
        std::cout << std::endl << "验证部分结果..." << std::endl;
        int verified = 0;
        int verify_errors = 0;
        int sample_size = std::min(100, stress_threads * stress_inserts);
        
        for (int i = 0; i < sample_size; ++i) {
            uint64_t key = static_cast<uint64_t>(i);
            uint64_t expected_value = key * 7;
            
            const uint64_t* value = stress_tree.Lookup(key);
            if (value && *value == expected_value) {
                verified++;
            } else {
                verify_errors++;
            }
        }
        
        std::cout << "验证样本: " << verified << "/" << sample_size << " 成功" << std::endl;
        if (verify_errors > 0) {
            std::cout << "验证错误: " << verify_errors << std::endl;
        }
    }
};

int main() {
    try {
        PerformanceComparison::RunComparison();
        std::cout << std::endl << "=== 测试完成 ===" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "测试异常: " << e.what() << std::endl;
        return 1;
    }
}
