#ifndef SEGMENTED_BLOCK_H_
#define SEGMENTED_BLOCK_H_

#include <vector>
#include <memory>
#include <atomic>
#include <cstdint>
#include "per_thread_data_block.h"
#include "constants.h"

// 前向声明
class BlockAllocator;

/// 分段块（论文3.3节，引用1-71、1-105）
/// 扩展：精准判断转换时机（基于块满阈值+键范围）
class SegmentedBlock {
 public:
  SegmentedBlock(size_t max_threads, BlockAllocator* allocator);

  PerThreadDataBlock* AllocatePerThreadBlock(size_t thread_id);
  const std::vector<std::unique_ptr<PerThreadDataBlock>>& GetAllPerThreadBlocks() const {
    return per_thread_blocks_;
  }

  /// 更新分段块的键范围（在插入时调用）
  void UpdateKeyRange(uint64_t key);

  /// 精准判断是否需要转换（论文4.2节：块满数量达标+存在键溢出）
  /// @param current_max_key 当前全局最大键
  bool NeedConversion(uint64_t current_max_key) const;

  uint64_t GetMinKey() const { return min_key_.load(std::memory_order_acquire); }
  uint64_t GetMaxKey() const { return max_key_.load(std::memory_order_acquire); }

 private:
  const size_t max_threads_;                          // 最大支持线程数
  std::vector<std::unique_ptr<PerThreadDataBlock>> per_thread_blocks_;  // 每线程块列表
  BlockAllocator* allocator_;                         // 块分配器
  std::atomic<uint64_t> min_key_;                     // 分段块内最小键（原子更新）
  std::atomic<uint64_t> max_key_;                     // 分段块内最大键（原子更新）
};

#endif  // SEGMENTED_BLOCK_H_
