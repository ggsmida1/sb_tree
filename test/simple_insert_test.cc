#include "sb_tree.h"
#include <iostream>
#include <thread>

int main() {
    std::cout << "Creating SBTree..." << std::endl;
    SBTree tree;
    
    std::cout << "Thread ID: " << std::this_thread::get_id() << std::endl;
    std::cout << "Thread hash: " << std::hash<std::thread::id>{}(std::this_thread::get_id()) << std::endl;
    
    std::cout << "Inserting key=100, value=1000..." << std::endl;
    bool result = tree.Insert(100, 1000);
    std::cout << "Insert result: " << (result ? "success" : "failed") << std::endl;
    
    std::cout << "Current max key: " << tree.GetCurrentMaxKey() << std::endl;
    
    std::cout << "Looking up key=100..." << std::endl;
    
    // 直接尝试在分段块中查找
    uint64_t seg_value = 0;
    bool found_in_seg = tree.LookupInSegmentedBlock(100, &seg_value);
    std::cout << "LookupInSegmentedBlock result: " << (found_in_seg ? "found" : "not found") << std::endl;
    if (found_in_seg) {
        std::cout << "Value in SegmentedBlock: " << seg_value << std::endl;
    }
    
    const uint64_t* value = tree.Lookup(100);
    
    if (value == nullptr) {
        std::cout << "Lookup failed: value is nullptr" << std::endl;
        return 1;
    }
    
    std::cout << "Lookup succeeded: value=" << *value << std::endl;
    
    if (*value != 1000) {
        std::cout << "ERROR: Expected 1000, got " << *value << std::endl;
        return 1;
    }
    
    std::cout << "Test PASSED!" << std::endl;
    return 0;
}
