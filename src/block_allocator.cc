#include "block_allocator.h"
#include <thread>
#include <cstdlib>

// 线程局部存储初始化
thread_local std::unique_ptr<BlockAllocator::PerThreadFreeList> BlockAllocator::tls_free_list_ = nullptr;

// -----------------------------------------------------------------------------
// BlockAllocator 实现（论文3.5节，引用1-89、1-90）
// -----------------------------------------------------------------------------
BlockAllocator::BlockAllocator(size_t block_size)
    : block_size_(block_size) {
  // 初始化全局备用列表（按CPU核心数）
  const size_t num_cores = std::thread::hardware_concurrency();
  global_free_lists_.reserve(num_cores);
  for (size_t i = 0; i < num_cores; ++i) {
    global_free_lists_.emplace_back(std::make_unique<PerThreadFreeList>());
  }
}

BlockAllocator::~BlockAllocator() {
  // 全局备用列表自动释放（PerThreadFreeList析构函数释放块）
}

BlockAllocator::PerThreadFreeList& BlockAllocator::GetPerThreadFreeList() {
  if (!tls_free_list_) {
    tls_free_list_ = std::make_unique<PerThreadFreeList>();
  }
  return *tls_free_list_;
}

void* BlockAllocator::Allocate() {
  auto& tls_list = GetPerThreadFreeList();
  // 先从线程局部列表分配（无锁）
  if (!tls_list.free_blocks.empty()) {
    void* block = tls_list.free_blocks.back();
    tls_list.free_blocks.pop_back();
    return block;
  }

  // 线程局部列表为空，从全局备用列表分配（引用1-90）
  const size_t core_idx = std::hash<std::thread::id>{}(std::this_thread::get_id()) % 
                          global_free_lists_.size();
  auto& global_list = *global_free_lists_[core_idx];
  std::lock_guard<std::mutex> lock(global_list.mutex);
  if (!global_list.free_blocks.empty()) {
    void* block = global_list.free_blocks.back();
    global_list.free_blocks.pop_back();
    return block;
  }

  // 全局列表也为空，动态分配（引用1-89）
  return malloc(block_size_);
}

void BlockAllocator::Deallocate(void* block) {
  if (block == nullptr) {
    return;
  }
  auto& tls_list = GetPerThreadFreeList();
  // 释放到线程局部列表（无锁）
  tls_list.free_blocks.push_back(block);

  // 线程局部列表过大，转移部分到全局列表（避免内存浪费）
  const size_t kMaxLocalBlocks = 1024;
  if (tls_list.free_blocks.size() > kMaxLocalBlocks) {
    const size_t transfer_count = tls_list.free_blocks.size() / 2;
    const size_t core_idx = std::hash<std::thread::id>{}(std::this_thread::get_id()) % 
                            global_free_lists_.size();
    auto& global_list = *global_free_lists_[core_idx];
    std::lock_guard<std::mutex> global_lock(global_list.mutex);
    for (size_t i = 0; i < transfer_count; ++i) {
      global_list.free_blocks.push_back(tls_list.free_blocks.back());
      tls_list.free_blocks.pop_back();
    }
  }
}
