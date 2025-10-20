#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <cassert>
#include "../include/sb_tree.h"

int main() {
    std::cout << "=== 转换器调试测试 ===" << std::endl;
    
    SBTree tree;
    const int num_threads = 4;
    const int inserts_per_thread = 1000;
    const int total_inserts = num_threads * inserts_per_thread;
    
    std::cout << "测试配置:" << std::endl;
    std::cout << "  线程数: " << num_threads << std::endl;
    std::cout << "  每线程插入数: " << inserts_per_thread << std::endl;
    std::cout << "  总插入数: " << total_inserts << std::endl;
    std::cout << std::endl;
    
    // 多线程插入
    std::cout << "开始多线程插入..." << std::endl;
    std::vector<std::thread> threads;
    std::atomic<int> insert_count{0};
    std::atomic<int> conversion_count{0};
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&tree, &insert_count, &conversion_count, t, inserts_per_thread]() {
            for (int i = 0; i < inserts_per_thread; ++i) {
                int key = t * inserts_per_thread + i;
                bool success = tree.Insert(key, key * 10);
                if (success) {
                    insert_count.fetch_add(1);
                }
                
                // 每10次插入检查一次转换状态
                if (i % 10 == 0) {
                    // 这里可以添加转换状态检查
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    std::cout << "插入完成，成功插入: " << insert_count.load() << std::endl;
    
    // 等待转换器
    std::cout << "等待转换器完成..." << std::endl;
    tree.WaitForConverterIdle();
    std::cout << "转换器空闲" << std::endl;
    
    // 验证结果
    std::cout << "验证结果..." << std::endl;
    int success_count = 0;
    int error_count = 0;
    for (int i = 0; i < total_inserts; ++i) {
        const uint64_t* value = tree.Lookup(i);
        if (value && *value == i * 10) {
            success_count++;
        } else {
            error_count++;
            if (error_count <= 5) {
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
    
    std::cout << "验证完成: " << success_count << "/" << total_inserts << " 成功" << std::endl;
    std::cout << "错误数量: " << error_count << std::endl;
    
    if (success_count == total_inserts) {
        std::cout << "✅ 测试通过" << std::endl;
        return 0;
    } else {
        std::cout << "❌ 测试失败，数据丢失" << std::endl;
        return 1;
    }
}
