#include "per_thread_data_block.h"
#include "block_allocator.h"

// -----------------------------------------------------------------------------
// PerThreadDataBlock 实现（论文3.3节，引用1-71、1-103）
// -----------------------------------------------------------------------------
PerThreadDataBlock::PerThreadDataBlock(BlockAllocator* allocator)
    : allocator_(allocator) {
  kv_pairs_.reserve(kPerThreadBlockCapacity);
}

bool PerThreadDataBlock::Insert(uint64_t key, uint64_t value) {
  if (IsFull()) {
    return false;
  }
  kv_pairs_.emplace_back(KeyValuePair{key, value});
  return true;
}
