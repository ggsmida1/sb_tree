#include "per_thread_data_block.h"
#include "block_allocator.h"
#include <iostream>

int main() {
    std::cout << "Creating BlockAllocator..." << std::endl;
    BlockAllocator allocator(4096);
    
    std::cout << "Creating PerThreadDataBlock..." << std::endl;
    PerThreadDataBlock pt_block(&allocator);
    
    std::cout << "Inserting key 100..." << std::endl;
    bool result = pt_block.Insert(100, 1000);
    std::cout << "Insert result: " << (result ? "success" : "failed") << std::endl;
    
    if (result) {
        std::cout << "Block size: " << pt_block.GetAllKv().size() << std::endl;
        std::cout << "Is full: " << (pt_block.IsFull() ? "yes" : "no") << std::endl;
    }
    
    return 0;
}
