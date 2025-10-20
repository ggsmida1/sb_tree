#ifndef BLOCK_ALLOCATOR_H_
#define BLOCK_ALLOCATOR_H_

#include <vector>
#include <memory>
#include <mutex>
#include <thread>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include "constants.h"

/// 块分配器（论文3.5节，引用1-89、1-90）
/// 扩展：每线程空闲列表（减少锁竞争）
class BlockAllocator {
 public:
  BlockAllocator(size_t block_size);
  ~BlockAllocator();

  void* Allocate();
  void Deallocate(void* block);

 private:
  /// 每线程空闲块列表（引用1-90）
  struct PerThreadFreeList {
    ~PerThreadFreeList() {
      for (void* block : free_blocks) {
        free(block);
      }
    }
    std::vector<void*> free_blocks;
    std::mutex mutex;
  };

  PerThreadFreeList& GetPerThreadFreeList();

  const size_t block_size_;                       // 固定块大小
  std::vector<std::unique_ptr<PerThreadFreeList>> global_free_lists_;  // 全局备用列表
  thread_local static std::unique_ptr<PerThreadFreeList> tls_free_list_;  // 线程局部列表
  
  // 预分配内存池（论文要求）
  void* pre_allocated_memory_;                    // 预分配的大块内存
  size_t memory_pool_size_;                       // 内存池大小
  std::atomic<size_t> memory_pool_offset_;        // 内存池偏移量
  std::mutex memory_pool_mutex_;                  // 内存池互斥锁
};

#endif  // BLOCK_ALLOCATOR_H_
