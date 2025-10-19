#include "sb_tree.h"
#include "segmented_block.h"
#include "per_thread_data_block.h"
#include "block_allocator.h"
#include <iostream>
#include <thread>

int main() {
    BlockAllocator allocator(4096);
    SegmentedBlock seg_block(kSegmentedBlockMaxThreads, &allocator);
    
    std::cout << "Initial state:" << std::endl;
    std::cout << "  MinKey: " << seg_block.GetMinKey() << std::endl;
    std::cout << "  MaxKey: " << seg_block.GetMaxKey() << std::endl;
    std::cout << "  NeedConversion(100): " << seg_block.NeedConversion(100) << std::endl;
    
    // 分配一个 PerThreadBlock 并插入数据
    size_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % kSegmentedBlockMaxThreads;
    PerThreadDataBlock* pt_block = seg_block.AllocatePerThreadBlock(thread_id);
    
    if (pt_block) {
        std::cout << "\nAllocated PerThreadBlock for thread " << thread_id << std::endl;
        pt_block->Insert(100, 1000);
        std::cout << "  Inserted key=100" << std::endl;
        
        // 更新键范围
        seg_block.UpdateKeyRange(100);
        std::cout << "  MinKey: " << seg_block.GetMinKey() << std::endl;
        std::cout << "  MaxKey: " << seg_block.GetMaxKey() << std::endl;
        std::cout << "  NeedConversion(100): " << seg_block.NeedConversion(100) << std::endl;
    }
    
    return 0;
}

