#include "segmented_block.h"
#include "block_allocator.h"
#include <algorithm>

// -----------------------------------------------------------------------------
// SegmentedBlock 实现（论文3.3节，引用1-71、1-105）
// -----------------------------------------------------------------------------
SegmentedBlock::SegmentedBlock(size_t max_threads, BlockAllocator* allocator)
    : max_threads_(max_threads),
      allocator_(allocator),
      min_key_(kInvalidKey),
      max_key_(0) {
  per_thread_blocks_.resize(max_threads);
}

PerThreadDataBlock* SegmentedBlock::AllocatePerThreadBlock(size_t thread_id) {
  if (thread_id >= max_threads_) {
    return nullptr;
  }
  
  // 如果已经存在，直接返回
  if (per_thread_blocks_[thread_id]) {
    return per_thread_blocks_[thread_id].get();
  }
  
  // 创建新的PerThreadBlock
  auto pt_block = std::make_unique<PerThreadDataBlock>(allocator_);
  PerThreadDataBlock* ptr = pt_block.get();
  per_thread_blocks_[thread_id] = std::move(pt_block);
  return ptr;
}

void SegmentedBlock::UpdateKeyRange(uint64_t key) {
  // 原子更新最小键
  uint64_t current_min = min_key_.load(std::memory_order_acquire);
  while (current_min == kInvalidKey || key < current_min) {
    if (min_key_.compare_exchange_weak(current_min, key, 
                                       std::memory_order_acq_rel, 
                                       std::memory_order_acquire)) {
      break;
    }
  }
  
  // 原子更新最大键
  uint64_t current_max = max_key_.load(std::memory_order_acquire);
  while (key > current_max) {
    if (max_key_.compare_exchange_weak(current_max, key, 
                                       std::memory_order_acq_rel, 
                                       std::memory_order_acquire)) {
      break;
    }
  }
}

bool SegmentedBlock::NeedConversion(uint64_t current_max_key) const {
  // 转换条件（论文4.2节）：
  // 1. 超过一半的PerThreadBlock满；2. 存在延迟数据
  size_t full_count = 0;
  bool has_delayed = false;
  const auto& pt_blocks = GetAllPerThreadBlocks();
  const uint64_t seg_min = min_key_.load(std::memory_order_acquire);
  
  for (const auto& pt_block : pt_blocks) {
    if (!pt_block) continue;
    if (pt_block->IsFull()) {
      full_count++;
    }
    const uint64_t pt_min = pt_block->GetMinKey();
    // 判断是否存在延迟数据：键小于分段块最小键
    // 或者键明显小于当前全局最大键（避免无符号整数下溢）
    if (pt_min != kInvalidKey) {
      if (pt_min < seg_min) {
        has_delayed = true;
      } else if (current_max_key > 1000 && pt_min < current_max_key - 1000) {
        has_delayed = true;
      }
    }
  }
  
  // 论文要求：超过一半（大于等于）的块满，或存在延迟数据
  return (full_count >= (max_threads_ + 1) / 2) || has_delayed;
}
