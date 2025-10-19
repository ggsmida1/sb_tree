#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <random>
#include <algorithm>
#include <iomanip>

class MultithreadedInsertTest {
public:
    static void RunTest() {
        std::cout << "=== SB-Tree 多线程插入性能测试 ===" << std::endl;
        
        // 测试参数
        const int num_threads = 8;
        const int inserts_per_thread = 10000;
        const int total_inserts = num_threads * inserts_per_thread;
        
        std::cout << "线程数: " << num_threads << std::endl;
        std::cout << "每线程插入数: " << inserts_per_thread << std::endl;
        std::cout << "总插入数: " << total_inserts << std::endl;
        std::cout << std::endl;
        
        // 测试1: 顺序插入（单线程基准）
        std::cout << "1. 单线程顺序插入测试..." << std::endl;
        auto start_time = std::chrono::high_resolution_clock::now();
        
        SBTree single_thread_tree;
        for (int i = 0; i < total_inserts; ++i) {
            single_thread_tree.Insert(i, i * 10);
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto single_thread_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "单线程耗时: " << single_thread_duration.count() << " ms" << std::endl;
        
        // 验证单线程结果
        int single_thread_verified = 0;
        for (int i = 0; i < total_inserts; ++i) {
            const uint64_t* value = single_thread_tree.Lookup(i);
            if (value && *value == static_cast<uint64_t>(i * 10)) {
                single_thread_verified++;
            }
        }
        std::cout << "单线程验证: " << single_thread_verified << "/" << total_inserts << " 成功" << std::endl;
        std::cout << std::endl;
        
        // 测试2: 多线程并发插入
        std::cout << "2. 多线程并发插入测试..." << std::endl;
        start_time = std::chrono::high_resolution_clock::now();
        
        SBTree multi_thread_tree;
        std::vector<std::thread> threads;
        std::atomic<int> successful_inserts{0};
        std::atomic<int> failed_inserts{0};
        
        // 启动多个线程
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis(0, total_inserts - 1);
                
                for (int i = 0; i < inserts_per_thread; ++i) {
                    // 使用线程ID和插入序号生成唯一键
                    uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
                    uint64_t value = key * 10;
                    
                    if (multi_thread_tree.Insert(key, value)) {
                        successful_inserts.fetch_add(1);
                    } else {
                        failed_inserts.fetch_add(1);
                    }
                }
            });
        }
        
        // 等待所有线程完成
        for (auto& thread : threads) {
            thread.join();
        }
        
        end_time = std::chrono::high_resolution_clock::now();
        auto multi_thread_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        std::cout << "多线程耗时: " << multi_thread_duration.count() << " ms" << std::endl;
        std::cout << "成功插入: " << successful_inserts.load() << std::endl;
        std::cout << "失败插入: " << failed_inserts.load() << std::endl;
        std::cout << std::endl;
        
        // 验证多线程结果
        std::cout << "3. 多线程结果验证..." << std::endl;
        int multi_thread_verified = 0;
        int verification_errors = 0;
        
        for (int t = 0; t < num_threads; ++t) {
            for (int i = 0; i < inserts_per_thread; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
                uint64_t expected_value = key * 10;
                
                const uint64_t* value = multi_thread_tree.Lookup(key);
                if (value && *value == expected_value) {
                    multi_thread_verified++;
                } else {
                    verification_errors++;
                    if (verification_errors <= 5) {  // 只显示前5个错误
                        std::cout << "验证错误: key=" << key << ", expected=" << expected_value;
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
        
        std::cout << "多线程验证: " << multi_thread_verified << "/" << total_inserts << " 成功" << std::endl;
        if (verification_errors > 5) {
            std::cout << "还有 " << (verification_errors - 5) << " 个验证错误..." << std::endl;
        }
        std::cout << std::endl;
        
        // 性能分析
        std::cout << "=== 性能分析 ===" << std::endl;
        double speedup = static_cast<double>(single_thread_duration.count()) / multi_thread_duration.count();
        double throughput_single = static_cast<double>(total_inserts) / single_thread_duration.count() * 1000;
        double throughput_multi = static_cast<double>(successful_inserts.load()) / multi_thread_duration.count() * 1000;
        
        std::cout << "加速比: " << std::fixed << std::setprecision(2) << speedup << "x" << std::endl;
        std::cout << "单线程吞吐量: " << std::fixed << std::setprecision(0) << throughput_single << " ops/sec" << std::endl;
        std::cout << "多线程吞吐量: " << std::fixed << std::setprecision(0) << throughput_multi << " ops/sec" << std::endl;
        std::cout << "吞吐量提升: " << std::fixed << std::setprecision(2) << (throughput_multi / throughput_single) << "x" << std::endl;
        
        // 测试3: 高并发压力测试
        std::cout << std::endl << "4. 高并发压力测试..." << std::endl;
        RunStressTest();
    }
    
private:
    static void RunStressTest() {
        const int stress_threads = 16;
        const int stress_inserts_per_thread = 5000;
        
        std::cout << "压力测试: " << stress_threads << " 线程, 每线程 " << stress_inserts_per_thread << " 次插入" << std::endl;
        
        SBTree stress_tree;
        std::vector<std::thread> stress_threads_vec;
        std::atomic<int> stress_success{0};
        std::atomic<int> stress_failures{0};
        
        auto stress_start = std::chrono::high_resolution_clock::now();
        
        for (int t = 0; t < stress_threads; ++t) {
            stress_threads_vec.emplace_back([&, t]() {
                for (int i = 0; i < stress_inserts_per_thread; ++i) {
                    uint64_t key = static_cast<uint64_t>(t) * stress_inserts_per_thread + i;
                    uint64_t value = key * 7;  // 使用不同的乘数避免冲突
                    
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
        
        auto stress_end = std::chrono::high_resolution_clock::now();
        auto stress_duration = std::chrono::duration_cast<std::chrono::milliseconds>(stress_end - stress_start);
        
        std::cout << "压力测试耗时: " << stress_duration.count() << " ms" << std::endl;
        std::cout << "成功: " << stress_success.load() << ", 失败: " << stress_failures.load() << std::endl;
        
        double stress_throughput = static_cast<double>(stress_success.load()) / stress_duration.count() * 1000;
        std::cout << "压力测试吞吐量: " << std::fixed << std::setprecision(0) << stress_throughput << " ops/sec" << std::endl;
    }
};

int main() {
    try {
        MultithreadedInsertTest::RunTest();
        std::cout << std::endl << "=== 测试完成 ===" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "测试异常: " << e.what() << std::endl;
        return 1;
    }
}
