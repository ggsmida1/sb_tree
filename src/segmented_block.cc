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

bool SegmentedBlock::NeedConversion(uint64_t current_max_key) const {
  // 转换条件（论文4.2节）：
  // 1. 超过一半的PerThreadBlock满；2. 存在键小于当前全局最大键（延迟数据）
  size_t full_count = 0;
  bool has_delayed = false;
  const auto& pt_blocks = GetAllPerThreadBlocks();
  for (const auto& pt_block : pt_blocks) {
    if (!pt_block) continue;
    if (pt_block->IsFull()) {
      full_count++;
    }
    const uint64_t pt_min = pt_block->GetMinKey();
    if (pt_min != kInvalidKey && pt_min < current_max_key) {
      has_delayed = true;
    }
  }
  return (full_count > max_threads_ / 2) || has_delayed;
}
