#include "sb_tree.h"
#include <iostream>
#include <thread>
#include <vector>

int main() {
    std::cout << "=== 简单调试测试 ===" << std::endl;
    
    SBTree tree;
    
    // 单线程测试，逐步增加数据量
    std::cout << "开始单线程插入测试..." << std::endl;
    
    for (int i = 0; i < 10000; ++i) {
        if (i % 1000 == 0) {
            std::cout << "插入进度: " << i << "/10000" << std::endl;
        }
        
        if (!tree.Insert(i, i * 10)) {
            std::cout << "插入失败: key=" << i << std::endl;
            return 1;
        }
    }
    
    std::cout << "单线程插入完成" << std::endl;
    
    // 验证数据
    std::cout << "开始验证数据..." << std::endl;
    for (int i = 0; i < 10000; ++i) {
        const uint64_t* value = tree.Lookup(i);
        if (!value || *value != static_cast<uint64_t>(i * 10)) {
            std::cout << "验证失败: key=" << i << std::endl;
            return 1;
        }
    }
    
    std::cout << "数据验证完成" << std::endl;
    
    // 多线程测试，从小规模开始
    std::cout << "开始多线程测试..." << std::endl;
    
    SBTree multi_tree;
    std::vector<std::thread> threads;
    const int num_threads = 2;  // 只使用2个线程
    const int inserts_per_thread = 1000;  // 每线程只插入1000个
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&multi_tree, t, inserts_per_thread]() {
            std::cout << "线程 " << t << " 开始插入..." << std::endl;
            
            for (int i = 0; i < inserts_per_thread; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
                uint64_t value = key * 10;
                
                if (!multi_tree.Insert(key, value)) {
                    std::cout << "线程 " << t << " 插入失败: key=" << key << std::endl;
                    return;
                }
                
                if (i % 200 == 0 && i > 0) {
                    std::cout << "线程 " << t << " 进度: " << i << "/" << inserts_per_thread << std::endl;
                }
            }
            
            std::cout << "线程 " << t << " 完成插入" << std::endl;
        });
    }
    
    for (auto& thread :) {
        thread.join();
    }
    
    std::cout << "多线程插入完成" << std::endl;
    
    // 验证多线程结果
    std::cout << "开始验证多线程结果..." << std::endl;
    for (int t = 0; t < num_threads; ++t) {
        for (int i = 0; i < inserts_per_thread; ++i) {
            uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
            uint64_t expected_value = key * 10;
            
            const uint64_t* value = multi_tree.Lookup(key);
            if (!value || *value != expected_value) {
                std::cout << "多线程验证失败: key=" << key << std::endl;
                return 1;
            }
        }
    }
    
    std::cout << "多线程验证完成" << std::endl;
    std::cout << "=== 测试成功 ===" << std::endl;
    
    return 0;
}
