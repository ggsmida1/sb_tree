#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <cassert>

// 调试版本的多线程测试
class DebugMultithreadedTest {
public:
    static void RunDebugTest() {
        std::cout << "=== SB-Tree 调试多线程测试 ===" << std::endl;
        
        // 小规模测试，逐步增加
        const int num_threads = 8;  // 增加到8线程
        const int inserts_per_thread = 5000;  // 增加插入数量
        
        std::cout << "线程数: " << num_threads << std::endl;
        std::cout << "每线程插入数: " << inserts_per_thread << std::endl;
        std::cout << std::endl;
        
        SBTree tree;
        std::vector<std::thread> threads;
        std::atomic<int> successful_inserts{0};
        std::atomic<int> failed_inserts{0};
        std::atomic<int> thread_counter{0};
        
        // 启动多个线程
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                int local_success = 0;
                int local_failures = 0;
                
                std::cout << "线程 " << t << " 开始插入..." << std::endl;
                
                for (int i = 0; i < inserts_per_thread; ++i) {
                    uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
                    uint64_t value = key * 10;
                    
                    if (tree.Insert(key, value)) {
                        local_success++;
                    } else {
                        local_failures++;
                        std::cout << "线程 " << t << " 插入失败: key=" << key << std::endl;
                    }
                    
                    // 每100次插入打印一次进度
                    if (i % 100 == 0 && i > 0) {
                        std::cout << "线程 " << t << " 进度: " << i << "/" << inserts_per_thread << std::endl;
                    }
                }
                
                successful_inserts.fetch_add(local_success);
                failed_inserts.fetch_add(local_failures);
                thread_counter.fetch_add(1);
                
                std::cout << "线程 " << t << " 完成: 成功=" << local_success << ", 失败=" << local_failures << std::endl;
            });
        }
        
        // 等待所有线程完成
        for (auto& thread : threads) {
            thread.join();
        }
        
        std::cout << std::endl;
        std::cout << "=== 测试结果 ===" << std::endl;
        std::cout << "成功插入: " << successful_inserts.load() << std::endl;
        std::cout << "失败插入: " << failed_inserts.load() << std::endl;
        
        // 简单验证
        std::cout << "开始验证..." << std::endl;
        int verified = 0;
        int errors = 0;
        
        for (int t = 0; t < num_threads; ++t) {
            for (int i = 0; i < inserts_per_thread; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
                uint64_t expected_value = key * 10;
                
                const uint64_t* value = tree.Lookup(key);
                if (value && *value == expected_value) {
                    verified++;
                } else {
                    errors++;
                    if (errors <= 5) {
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
        
        std::cout << "验证结果: " << verified << "/" << (num_threads * inserts_per_thread) << " 成功" << std::endl;
        if (errors > 5) {
            std::cout << "还有 " << (errors - 5) << " 个错误..." << std::endl;
        }
        
        std::cout << "=== 调试测试完成 ===" << std::endl;
    }
};

int main() {
    try {
        DebugMultithreadedTest::RunDebugTest();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "调试测试异常: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "调试测试未知异常" << std::endl;
        return 1;
    }
}
