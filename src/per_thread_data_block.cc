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

void PerThreadDataBlock::CopyAllKvThreadSafe(std::vector<KeyValuePair>* out) const {
  if (!out) return;
  // WaitForQuiescent 之后 converter 再抓取数据，因此这里可以无锁拷贝
  out->insert(out->end(), kv_pairs_.begin(), kv_pairs_.end());
}
