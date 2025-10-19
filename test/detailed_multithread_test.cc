image.png#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>

int main() {
    std::cout << "=== 详细多线程插入分析 ===" << std::endl;
    
    const int num_threads = 2;  // 减少线程数便于分析
    const int inserts_per_thread = 100;
    
    std::cout << "线程数: " << num_threads << std::endl;
    std::cout << "每线程插入数: " << inserts_per_thread << std::endl;
    std::cout << std::endl;
    
    // 单线程测试
    std::cout << "1. 单线程测试..." << std::endl;
    SBTree single_tree;
    for (int i = 0; i < num_threads * inserts_per_thread; ++i) {
        bool result = single_tree.Insert(i, i * 10);
        if (!result) {
            std::cout << "单线程插入失败: " << i << std::endl;
        }
    }
    
    // 验证单线程
    int single_verified = 0;
    for (int i = 0; i < num_threads * inserts_per_thread; ++i) {
        const uint64_t* value = single_tree.Lookup(i);
        if (value && *value == static_cast<uint64_t>(i * 10)) {
            single_verified++;
        }
    }
    std::cout << "单线程验证: " << single_verified << "/" << (num_threads * inserts_per_thread) << " 成功" << std::endl;
    std::cout << std::endl;
    
    // 多线程测试
    std::cout << "2. 多线程测试..." << std::endl;
    SBTree multi_tree;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    std::atomic<int> fail_count{0};
    std::vector<std::vector<uint64_t>> thread_keys(num_threads);
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < inserts_per_thread; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
                uint64_t value = key * 10;
                
                if (multi_tree.Insert(key, value)) {
                    success_count.fetch_add(1);
                    thread_keys[t].push_back(key);
                } else {
                    fail_count.fetch_add(1);
                    std::cout << "线程 " << t << " 插入失败: key=" << key << std::endl;
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    std::cout << "成功插入: " << success_count.load() << std::endl;
    std::cout << "失败插入: " << fail_count.load() << std::endl;
    
    // 详细验证
    std::cout << "3. 详细验证..." << std::endl;
    int total_verified = 0;
    int total_errors = 0;
    
    for (int t = 0; t < num_threads; ++t) {
        int thread_verified = 0;
        int thread_errors = 0;
        
        for (int i = 0; i < inserts_per_thread; ++i) {
            uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
            uint64_t expected_value = key * 10;
            
            const uint64_t* value = multi_tree.Lookup(key);
            if (value && *value == expected_value) {
                thread_verified++;
                total_verified++;
            } else {
                thread_errors++;
                total_errors++;
                if (thread_errors <= 3) {
                    std::cout << "线程 " << t << " 错误: key=" << key << ", expected=" << expected_value;
                    if (value) {
                        std::cout << ", got=" << *value;
                    } else {
                        std::cout << ", got=nullptr";
                    }
                    std::cout << std::endl;
                }
            }
        }
        
        std::cout << "线程 " << t << " 验证: " << thread_verified << "/" << inserts_per_thread << " 成功" << std::endl;
    }
    
    std::cout << "总验证: " << total_verified << "/" << (num_threads * inserts_per_thread) << " 成功" << std::endl;
    std::cout << "总错误: " << total_errors << std::endl;
    
    // 检查当前分段块状态
    std::cout << std::endl << "4. 当前分段块状态..." << std::endl;
    std::cout << "当前最大键: " << multi_tree.GetCurrentMaxKey() << std::endl;
    
    return 0;
}
