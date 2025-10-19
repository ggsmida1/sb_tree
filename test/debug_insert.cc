#include "sb_tree.h"
#include "per_thread_data_block.h"
#include "segmented_block.h"
#include <iostream>
#include <thread>

int main() {
    std::cout << "=== Testing PerThreadDataBlock directly ===" << std::endl;
    
    // 测试 PerThreadDataBlock
    PerThreadDataBlock pt_block(nullptr);
    std::cout << "Created PerThreadDataBlock" << std::endl;
    
    bool insert_result = pt_block.Insert(100, 1000);
    std::cout << "Insert result: " << (insert_result ? "success" : "failed") << std::endl;
    std::cout << "IsFull: " << pt_block.IsFull() << std::endl;
    std::cout << "GetMinKey: " << pt_block.GetMinKey() << std::endl;
    
    const auto& kv_pairs = pt_block.GetAllKv();
    std::cout << "Number of KV pairs: " << kv_pairs.size() << std::endl;
    for (const auto& kv : kv_pairs) {
        std::cout << "  Key=" << kv.key << ", Value=" << kv.value << std::endl;
    }
    
    std::cout << "\n=== Testing full SBTree ===" << std::endl;
    SBTree tree;
    
    std::cout << "Thread hash: " << std::hash<std::thread::id>{}(std::this_thread::get_id()) << std::endl;
    std::cout << "kSegmentedBlockMaxThreads: " << kSegmentedBlockMaxThreads << std::endl;
    size_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % kSegmentedBlockMaxThreads;
    std::cout << "Thread ID mod: " << thread_id << std::endl;
    
    std::cout << "Inserting key=100, value=1000..." << std::endl;
    bool result = tree.Insert(100, 1000);
    std::cout << "Insert result: " << (result ? "success" : "failed") << std::endl;
    
    // 直接检查
    uint64_t value = 0;
    bool found = tree.LookupInSegmentedBlock(100, &value);
    std::cout << "LookupInSegmentedBlock: " << (found ? "found" : "not found") << std::endl;
    if (found) {
        std::cout << "Value: " << value << std::endl;
    }
    
    return found ? 0 : 1;
}

