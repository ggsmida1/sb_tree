#include "sb_tree.h"
#include <iostream>
#include <thread>

int main() {
    std::cout << "Creating SBTree..." << std::endl;
    SBTree tree;
    
    std::cout << "Current max key: " << tree.GetCurrentMaxKey() << std::endl;
    size_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % 80;
    std::cout << "Thread ID: " << thread_id << std::endl;
    
    std::cout << "Inserting key 100..." << std::endl;
    bool result = tree.Insert(100, 1000);
    std::cout << "Insert result: " << (result ? "success" : "failed") << std::endl;
    
    // 尝试插入其他键
    std::cout << "Inserting key 200..." << std::endl;
    bool result2 = tree.Insert(200, 2000);
    std::cout << "Insert result 2: " << (result2 ? "success" : "failed") << std::endl;
    
    std::cout << "Current max key after insert: " << tree.GetCurrentMaxKey() << std::endl;
    
    if (result) {
        std::cout << "Looking up key 100..." << std::endl;
        const uint64_t* value = tree.Lookup(100);
        if (value) {
            std::cout << "Found value: " << *value << std::endl;
        } else {
            std::cout << "Value not found" << std::endl;
        }
    }
    
    return 0;
}
