#ifndef PER_THREAD_DATA_BLOCK_H_
#define PER_THREAD_DATA_BLOCK_H_

#include <vector>
#include <cstdint>
#include <algorithm>
#include <atomic>
#include "data_block.h"  // 包含KeyValuePair定义
#include "constants.h"

// 前向声明
class BlockAllocator;

/// 每线程数据块（论文3.3节，引用1-71、1-103）
/// 扩展：支持块清空复用（减少分配开销）
class PerThreadDataBlock {
 public:
  explicit PerThreadDataBlock(BlockAllocator* allocator);

  bool Insert(uint64_t key, uint64_t value);
  const std::vector<KeyValuePair>& GetAllKv() const { return kv_pairs_; }
  void CopyAllKvThreadSafe(std::vector<KeyValuePair>* out) const;
  bool IsFull() const { return kv_pairs_.size() >= kPerThreadBlockCapacity; }
  void Clear() { kv_pairs_.clear(); }  // 复用块时清空（引用1-111）
  uint64_t GetMinKey() const { 
    return kv_pairs_.empty() ? kInvalidKey : 
           (*std::min_element(kv_pairs_.begin(), kv_pairs_.end(), 
                              [](const KeyValuePair& a, const KeyValuePair& b) {
                                return a.key < b.key;
                              })).key;
  }

 private:
  std::vector<KeyValuePair> kv_pairs_;  // 无序存储（转换时排序，引用1-108）
  BlockAllocator* allocator_;           // 块分配器
  mutable std::atomic_flag write_lock_ = ATOMIC_FLAG_INIT; // 轻量自旋锁
  void Lock() const {
    while (write_lock_.test_and_set(std::memory_order_acquire)) {}
  }
  void Unlock() const {
    write_lock_.clear(std::memory_order_release);
  }
};

#endif  // PER_THREAD_DATA_BLOCK_H_
