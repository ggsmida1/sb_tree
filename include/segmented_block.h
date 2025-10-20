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
  ~SegmentedBlock();

  PerThreadDataBlock* AllocatePerThreadBlock(size_t thread_id);

  // 加载指定slot（仅读取，不转移所有权）
  PerThreadDataBlock* LoadPerThreadBlock(size_t thread_id) const;

  // 原子夺取指定slot（转换时使用，转移所有权给调用方）
  PerThreadDataBlock* StealPerThreadBlock(size_t thread_id);

  size_t GetMaxThreads() const { return max_threads_; }

  /// 更新分段块的键范围（在插入时调用）
  void UpdateKeyRange(uint64_t key);

  /// 精准判断是否需要转换（论文4.2节：块满数量达标+存在键溢出）
  /// @param current_max_key 当前全局最大键
  bool NeedConversion(uint64_t current_max_key) const;
  void MarkConversionTriggered();

  // 写者协调（用于安全转换）
  bool BeginWrite();  // 返回是否成功获取写锁
  void EndWrite();
  void WaitForQuiescent() const;

  uint64_t GetMinKey() const { return min_key_.load(std::memory_order_acquire); }
  uint64_t GetMaxKey() const { return max_key_.load(std::memory_order_acquire); }

 private:
  const size_t max_threads_;                          // 最大支持线程数
  std::unique_ptr<std::atomic<PerThreadDataBlock*>[]> per_thread_blocks_;  // 每线程块列表（原子指针数组）
  BlockAllocator* allocator_;                         // 块分配器
  std::atomic<uint64_t> min_key_;                     // 分段块内最小键（原子更新）
  std::atomic<uint64_t> max_key_;                     // 分段块内最大键（原子更新）
  alignas(64) std::atomic<uint32_t> active_writers_{0};           // 活跃写者计数（避免伪共享）
  alignas(64) std::atomic<uint64_t> last_conversion_ns_{0};        // 上次转换触发时间（ns）
  alignas(64) std::atomic<bool> conversion_triggered_{false};     // 修复P0-2：转换触发标记
};

/// 写者作用域守卫：构造BeginWrite，析构EndWrite，确保成对调用
class ScopedSegmentWrite {
 public:
  explicit ScopedSegmentWrite(SegmentedBlock* seg) : seg_(seg), acquired_(false) {
    if (seg_) {
      acquired_ = seg_->BeginWrite();
    }
  }
  ~ScopedSegmentWrite() {
    if (seg_ && acquired_) {
      seg_->EndWrite();
    }
  }
  
  // 检查是否成功获取了写锁
  bool IsAcquired() const { return acquired_; }
  ScopedSegmentWrite(const ScopedSegmentWrite&) = delete;
  ScopedSegmentWrite& operator=(const ScopedSegmentWrite&) = delete;
  ScopedSegmentWrite(ScopedSegmentWrite&& other) noexcept : seg_(other.seg_), acquired_(other.acquired_) { 
    other.seg_ = nullptr; 
    other.acquired_ = false;
  }
  ScopedSegmentWrite& operator=(ScopedSegmentWrite&& other) noexcept {
    if (this != &other) {
      if (seg_ && acquired_) seg_->EndWrite();
      seg_ = other.seg_;
      acquired_ = other.acquired_;
      other.seg_ = nullptr;
      other.acquired_ = false;
    }
    return *this;
  }
 private:
  SegmentedBlock* seg_;
  bool acquired_;  // 是否成功获取了写锁
};

#endif  // SEGMENTED_BLOCK_H_
